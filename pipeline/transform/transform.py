"""
Data transformation for SEC ADV data
"""

import logging

import pandas as pd

from pipeline.config.run_configuration import RunConfiguration
from pipeline.transform.base_transformer import BaseTransformer


class DataAnalyzer(BaseTransformer):
    """Analyzer for SEC ADV data"""

    def __init__(self, config: RunConfiguration):
        super().__init__(config)
        self.logger = logging.getLogger(__name__)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Perform data analysis on SEC ADV data"""
        self.logger.info("Analyzing SEC ADV data...")

        # Clean and _transform the data
        # These are simplified examples - real implementation would be more sophisticated

        # Convert AUM to numeric values for analysis
        data['aum_numeric'] = data['aum'].apply(self._convert_aum_to_numeric)

        # Analyze compensation arrangements
        data['comp_count'] = data['compensation_arrangements'].apply(
            lambda x: len(x.split(',')) if pd.notna(x) else 0
        )

        # Count number of private funds
        data['fund_count'] = data['private_funds'].apply(
            lambda x: x.count(',') + 1 if pd.notna(x) and x != 'N/A' else 0
        )

        # Calculate an aggregate score (simplified example)
        data['score'] = (
                data['aum_numeric'] / 1e9 * 0.6 +  # 60% weight to AUM
                data['employee_count'] * 0.2 +  # 20% weight to employees
                data['fund_count'] * 0.2  # 20% weight to number of funds
        )

        # Sort by score
        return data.sort_values('score', ascending=False)

    def _convert_aum_to_numeric(self, aum_str: str) -> float:
        """Convert AUM string to numeric value"""
        if pd.isna(aum_str) or aum_str == 'Unknown' or aum_str == 'N/A':
            return 0.0

        # Remove $ and commas
        aum_str = aum_str.replace('$', '').replace(',', '')

        # Convert based on suffix
        if 'billion' in aum_str:
            return float(aum_str.replace('billion', '').strip()) * 1e9
        elif 'million' in aum_str:
            return float(aum_str.replace('million', '').strip()) * 1e6
        elif 'thousand' in aum_str:
            return float(aum_str.replace('thousand', '').strip()) * 1e3
        else:
            try:
                return float(aum_str)
            except ValueError:
                return 0.0
