"""
Excel writer for MLP pipeline
"""

import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd

from pipeline.config.run_configuration import RunConfiguration


class ExcelWriter:
    """Excel writer for MLP pipeline that writes tables to separate sheets"""

    def __init__(self, config: RunConfiguration):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def write(self, data_dict: Dict[str, pd.DataFrame]) -> str:
        """
        Write multiple dataframes to separate sheets in an Excel file

        Args:
            data_dict: Dictionary mapping sheet names to pandas DataFrames

        Returns:
            Path to the created Excel file
        """
        self.logger.info(f"Writing {len(data_dict)} tables to Excel file...")

        # Ensure output directory exists
        os.makedirs(self.config.output_dir, exist_ok=True)

        # Format timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(self.config.output_dir, f'sec_adv_report_{timestamp}.xlsx')

        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Process each dataframe
            for sheet_name, df in data_dict.items():
                # Skip empty dataframes
                if df.empty:
                    continue

                # Format specific columns
                df_formatted = self._format_dataframe(df, sheet_name)

                # Write to Excel
                self.logger.info(f"Writing sheet: {sheet_name} with {len(df_formatted)} rows")
                df_formatted.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-adjust columns width
                self._adjust_column_width(writer, sheet_name, df_formatted)

        self.logger.info(f"Excel report generated at {output_path}")
        return output_path

    def _format_dataframe(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Format specific columns based on sheet name"""
        df_copy = df.copy()

        if sheet_name == 'Client_Types' and 'aum_value' in df_copy.columns:
            # Format AUM as currency
            df_copy['aum_value'] = df_copy['aum_value'].apply(
                lambda x: f"${int(x):,}" if pd.notna(x) and x != 'None' else "$0"
            )

        # Format date columns across all tables
        for col in df_copy.columns:
            if 'date' in col.lower() or col in ('created_at', 'updated_at'):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='ignore')
                if df_copy[col].dtype.kind == 'M':  # If successfully converted to datetime
                    df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        return df_copy

    def _adjust_column_width(self, writer, sheet_name: str, df: pd.DataFrame):
        """Adjust column width based on content"""
        worksheet = writer.sheets[sheet_name]
        for i, col in enumerate(df.columns):
            # Calculate maximum column width
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            ) + 2  # Add a little extra space

            # Excel has column width limits
            col_letter = chr(65 + i) if i < 26 else chr(64 + i // 26) + chr(65 + i % 26)
            worksheet.column_dimensions[col_letter].width = min(max_length, 50)
