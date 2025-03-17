"""
Data source for SEC ADV metadata
"""

import gzip
import io
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests

from pipeline.config.run_configuration import RunConfiguration
from pipeline.source.base_source import BaseSource


class Source(BaseSource):
    """Data source for SEC ADV metadata"""

    def __init__(self, config: RunConfiguration):
        super().__init__()
        self.config = config

    def run(self, as_of_date: datetime, firm_crd_numbers: List[int]) -> Dict[int, str]:
        df_metadata = self.download_metadata(as_of_date)
        return self.download_pdf(df_metadata, firm_crd_numbers)

    def download_metadata(self, as_of_date: datetime) -> pd.DataFrame:
        """Download metadata from SEC"""
        as_of_date_str = as_of_date.strftime('%m_%d_%Y')
        self.logger.info(f"Downloading metadata for {as_of_date_str}")

        path = rf"https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_FIRM_SEC_Feed_{as_of_date_str}.xml.gz"

        try:
            response = requests.get(path)

            if response.status_code == 200:
                # Unzip the content
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                    # Ensure the content is read as a string with the correct encoding
                    xml_content = gz.read().decode('ISO-8859-1')

                # Read the data into a dataframe specifying the encoding explicitly if needed
                df = pd.read_xml(io.StringIO(xml_content), xpath='//Info', encoding='ISO-8859-1')

                # Apply metadata transformer
                return self._transform_metadata(df)
            else:
                self.logger.error(f"Failed to download metadata. Status code: {response.status_code}")
                if self.config.raise_error:
                    raise Exception(f"Failed to download metadata. Status code: {response.status_code}")

        except Exception as e:
            self.logger.error(f"Error downloading metadata: {str(e)}")
            if self.config.raise_error:
                raise

    def _transform_metadata(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform SEC ADV metadata"""
        self.logger.info("Transforming SEC ADV metadata...")

        # Create a column with paths for the PDFs
        data['DownloadPath'] = data['FirmCrdNb'].apply(
            lambda x: f"https://reports.adviserinfo.sec.gov/reports/ADV/{x}/PDF/{x}.pdf"
        )
        data.set_index('FirmCrdNb', inplace=True)

        return data

    def download_pdf(self, df_metadata: pd.DataFrame, firm_crd_numbers: List[int]) -> Dict[int, str]:
        """Download PDF files for the specified firm CRD numbers"""
        self.logger.info("Downloading PDF files...")
        result_paths = {}

        for firm_crd in firm_crd_numbers:
            try:
                # Validate the firm CRD exists in metadata
                if firm_crd not in df_metadata.index:
                    self.logger.warning(f"Firm CRD {firm_crd} not found in metadata")
                    continue

                # Prepare file paths
                url = df_metadata.loc[firm_crd]['DownloadPath']
                file_name = url.split('/')[-1]
                save_path = os.path.join(self.config.input_dir, file_name)

                # Check if the file already exists
                if os.path.isfile(save_path):
                    self.logger.info(f"File {file_name} already exists. Using existing file.")
                    result_paths[firm_crd] = save_path
                else:
                    # Download the file
                    if self._download_file(url, save_path):
                        result_paths[firm_crd] = save_path
                    elif self.config.raise_error:
                        self.logger.error(f"Failed to download file for Firm CRD {firm_crd}")
                        raise Exception(f"Failed to download file for Firm CRD {firm_crd}")

            except Exception as e:
                self.logger.error(f"Error processing Firm CRD {firm_crd}: {str(e)}")
                if self.config.raise_error:
                    raise

        return result_paths

    def _download_file(self, url: str, save_path: str) -> bool:
        """Download a file from a URL and save it locally"""
        try:
            # Request the file
            response = requests.get(url)

            # Check response status
            if response.status_code == 200:
                # Write file to disk
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                self.logger.info(f"Downloaded {save_path}")
                return True
            else:
                self.logger.error(f"Failed to download from {url}. Status code: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return False
