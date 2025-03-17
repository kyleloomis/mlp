"""
Fund analysis transformer for MLP pipeline
"""

from typing import Dict

import pandas as pd

from pipeline.config.run_configuration import RunConfiguration
from pipeline.transform.base_transformer import BaseTransformer


class FundAnalysisTransformer(BaseTransformer):
    """Transformer for analyzing fund performance data"""

    def __init__(self, config: RunConfiguration):
        super().__init__(config)

    def transform(self, data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Transform and analyze the joined data from database

        Args:
            data: DataFrame from query_all() with joined table data

        Returns:
            Dictionary of DataFrames with analysis results
        """
        self.logger.info("Starting fund analysis transformation...")

        # Clean the data and prepare for analysis
        cleaned_data = self._clean_data(data)

        # Perform analyses
        results = {}
        results["top_funds"] = self.get_top_funds(cleaned_data)
        results["aum_by_client_type"] = self.analyze_client_distribution(cleaned_data)

        return results

    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare the data for analysis"""
        # Make a copy to avoid modifying the original
        df = data.copy()

        # Convert AUM values to numeric
        if 'aum_value' in df.columns:
            df['aum_value'] = pd.to_numeric(
                df['aum_value'].astype(str).str.replace(r'[^\d.]', '', regex=True),
                errors='coerce'
            )

        return df

    def get_top_funds(self, data: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """
        Identify top-performing funds based on AUM values

        Args:
            data: DataFrame with fund data
            top_n: Number of top funds to return

        Returns:
            DataFrame with top funds by AUM
        """
        self.logger.info(f"Identifying top {top_n} funds by AUM...")

        # Group by firm and fund
        fund_data = data.dropna(subset=['fund_name', 'fund_id'])

        # Calculate total AUM per firm
        firm_aum = {}
        for firm_crd in fund_data['firm_crd_nb'].unique():
            firm_rows = data[data['firm_crd_nb'] == firm_crd]
            firm_aum[firm_crd] = firm_rows['aum_value'].sum()

        # Create fund performance DataFrame
        fund_metrics = []
        for (firm_crd, fund_name), group in fund_data.groupby(['firm_crd_nb', 'fund_name']):
            fund_metrics.append({
                'firm_crd_nb': firm_crd,
                'business_name': group['business_name'].iloc[0],
                'fund_name': fund_name,
                'fund_id': group['fund_id'].iloc[0],
                'total_firm_aum': firm_aum.get(firm_crd, 0)
            })

        # Convert to DataFrame and sort by AUM
        performance_df = pd.DataFrame(fund_metrics)
        return performance_df.sort_values('total_firm_aum', ascending=False).head(top_n)

    def analyze_client_distribution(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze how AUM is distributed across different client types

        Args:
            data: DataFrame with client type data

        Returns:
            DataFrame with AUM by client type
        """
        self.logger.info("Analyzing AUM distribution by client type...")

        # Filter for rows with client type information
        client_data = data.dropna(subset=['client_type'])
        client_data = client_data[client_data['client_type'] != 'None']

        if client_data.empty:
            return pd.DataFrame()

        # Group by client type and calculate total AUM
        aum_by_type = client_data.groupby(['client_type'])['aum_value'].sum().reset_index()
        aum_by_type = aum_by_type.sort_values('aum_value', ascending=False)

        # Calculate percentage of total AUM
        total_aum = aum_by_type['aum_value'].sum()
        aum_by_type['percentage'] = (aum_by_type['aum_value'] / total_aum * 100).round(2)

        return aum_by_type
