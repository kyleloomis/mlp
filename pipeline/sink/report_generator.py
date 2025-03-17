"""
Report generator for MLP pipeline
"""

import logging
import os
from datetime import datetime

import pandas as pd

from pipeline.config.run_configuration import RunConfiguration


class ReportGenerator:
    """Generate reports from SEC ADV data"""
    
    def __init__(self, config: RunConfiguration):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def generate_excel_report(self, data: pd.DataFrame) -> str:
        """Generate an Excel report with SEC ADV data"""
        self.logger.info("Generating Excel report...")
        
        output_path = os.path.join(self.config.output_dir, 'sec_adv_report.xlsx')
        
        # Create a writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Write main data sheet
            data.to_excel(writer, sheet_name='Firm Data', index=False)
            
            # Create a summary sheet
            summary = pd.DataFrame({
                'Total Firms': [len(data)],
                'Average AUM': [data['aum_numeric'].mean()],
                'Total Employees': [data['employee_count'].sum()],
                'Total Funds': [data['fund_count'].sum()],
                'Report Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Create a top funds sheet
            if 'score' in data.columns:
                top_funds = data.sort_values('score', ascending=False).head(10)
                top_funds = top_funds[['firm_crd_nb', 'business_name', 'aum', 'fund_count', 'score']]
                top_funds.to_excel(writer, sheet_name='Top Funds', index=False)
        
        self.logger.info(f"Excel report generated at {output_path}")
        return output_path