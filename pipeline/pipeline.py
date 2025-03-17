"""
Main MLP pipeline
"""

import logging
from datetime import datetime

import uvicorn

from api.service import APIService
from pipeline.config.run_configuration import RunConfiguration
from pipeline.sink.database_sink import DatabaseSink
from pipeline.sink.excel_writer import ExcelWriter
from pipeline.source.source import Source
from pipeline.transform.fund_analysis_transformer import FundAnalysisTransformer
from pipeline.transform.pdf_transformer import PdfTransformer
from pipeline.transform.utils import prepare_tables_for_excel


class MLPPipeline:
    """Main MLP pipeline class"""

    def __init__(self, config: RunConfiguration):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # components
        self.source = Source(config)
        self.pdf_transformer = PdfTransformer(config)
        self.fund_analysis_transformer = FundAnalysisTransformer(config)
        self.db_manager = DatabaseSink(config)
        self.excel_writer = ExcelWriter(config)
        self.api_service = APIService(config)

    def run(self, as_of_date: datetime):
        """Run the full pipeline"""
        self.logger.info("Starting MLP pipeline...")

        # (1) Download metadata and PDFs
        pdf_paths = self.source.run(as_of_date, self.config.firm_crd_numbers)

        # (2) Extract and Store Information
        firm_data = {firm_crd_number: self.pdf_transformer.transform(firm_crd_number) for firm_crd_number, path in
                     pdf_paths.items()}
        self.db_manager.write(firm_data)

        # (3): Data Transformation and Analysis
        df = self.db_manager.query_all()
        analysis = self.fund_analysis_transformer.transform(df)
        print(analysis['top_funds'])
        print(analysis['aum_by_client_type'])

        # (4): Generate Excel File
        tables = prepare_tables_for_excel(df)
        self.excel_writer.write(tables)

        # (5): Discussion: Scalability and Performance
        # I have designed the data pipeline to be broken down into 3 main parts: source, transform, and sink. In order to improve scalability and performance, I would improve each part separately, introducing new frameworks/technologies that enable the pipeline to be horizontally scaled.
        # First, the data could be sourced (downloaded) concurrently using either multi-threading, where each thread would be used to request and save a PDF, or an async request pool, a non-blocking protocol useful for interacting with external systems.
        # Second, the transformation layer could be broken up into multiple pieces. I would leverage better OCR tools for PDF text extraction, such as a fine-tuned version of Mistral's OCR LLM. This enables PDFs to be processed more accurately and concurrently in the cloud. The resulting text blobs could then be extracted concurrently using either a multi-threading approach, or better yet, a distributed framework like Spark. Pandas is limited to in-memory processing, whereas Spark can be deployed on a massive compute cluster to horizontally scale to support the size of the workload.
        # Third, the database is the biggest bottleneck in the sink layer. I would replace SQLite with a cloud data warehouse such as BigQuery or Snowflake. This enables massive scalability for handling terabytes or even petabytes of data.
        # Finally, I would tie together these pieces of technology with an orchestration framework such as Airflow, ensuring that the pipeline runs whenever new data arrives and failures are handled properly.

        # (6): Integration with External Systems
        # Optionally run API server
        uvicorn.run(self.api_service.app, host="0.0.0.0", port=9999)

        # (7): Discussion: Automated Testing and Data Quality
        # For automated testing, I would use pytest to create unit tests for individual components, integration tests for interactions between pipeline stages, and end-to-end tests simulating the full workflow. These tests would run automatically in a CI/CD pipeline.
        # For data quality, I would implement validation at multiple stages: schema validation using Pydantic to enforce data structure, content validation to check for inconsistencies or errors, and statistical validation to identify outliers or anomalous patterns.
        # For monitoring, I would implement dashboards to track key quality metrics and data lineage, with automated alerting for any quality issues. Finally, I would document data quality SLAs and regularly review quality metrics to ensure continuous improvement of both the pipeline and the testing framework.

        # (8): Discussion: Identify Top Performing Funds
        # To identify top-performing funds, I would enhance the existing database to incorporate risk-adjusted return metrics beyond AUM, such as Sharpe ratio. The Sharpe Ratio is more useful since it indicates the reward per unit of risk taken with the strategy. The max drawdown since inception would also be useful to asses the worst historical performance.
        # Questions:
        # * How long has the fund been around?
        # * What is the Sharpe ratio over the life period of the fund?
        # * What is the benchmark (e.g. index) to compare the fund to?
        # * Has the fund maintained consistent performance through different market conditions?
        # * What is the maximum drawdown the fund has experienced?
        # Additional data: historical returns (ideally monthly), volatility metrics, benchmark performance data, and information about market conditions during the evaluation period.

        # TOTAL TIME FOR COMPLETION: ~6 hours

        self.logger.info("MLP pipeline completed successfully")
