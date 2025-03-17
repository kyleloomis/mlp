"""
Main entry point for MLP pipeline
"""

import argparse
import os
from datetime import datetime

import uvicorn

from pipeline.api.service import APIService
from pipeline.config.run_configuration import RunConfiguration
from pipeline.pipeline import MLPPipeline


def main():
    """Main entry point for the MLP pipeline"""
    parser = argparse.ArgumentParser(description='SEC ADV Data Pipeline')
    parser.add_argument('--working-date', type=str, help='Working date in YYYY-MM-DD format')
    parser.add_argument('--input-dir', type=str, default='./data/input', help='Input directory for downloaded files')
    parser.add_argument('--output-dir', type=str, default='./data/output', help='Output directory for reports')
    parser.add_argument('--firms', type=str, nargs='+', default=['160882', '160021', '1679500'], 
                        help='Firm CRD numbers to process')
    parser.add_argument('--db-path', type=str, default='./data/sec_adv.db', help='Path to SQLite database')
    parser.add_argument('--log-path', type=str, default='./logs/pipeline.log', help='Path to log file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--run-api', action='store_true', help='Run the API server')
    parser.add_argument('--api-port', type=int, default=8000, help='Port for API server')
    
    args = parser.parse_args()
    
    # Parse working date or use default
    if args.working_date:
        working_date = datetime.strptime(args.working_date, '%Y-%m-%d')
    else:
        working_date = datetime(2025, 2, 14)  # Default from notebook
    
    # Create directories
    os.makedirs(os.path.dirname(args.log_path), exist_ok=True)
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    
    # Initialize configuration
    config = RunConfiguration(
        as_of_date=working_date,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        firm_crd_numbers=args.firms,
        db_path=args.db_path,
        log_path=args.log_path,
        verbose=args.verbose
    )
    
    # Run pipeline
    pipeline = MLPPipeline(config)
    result = pipeline.run()
    
    print(f"Pipeline completed: {result}")
    
    # Optionally run API server
    if args.run_api:
        api_service = APIService(config)
        uvicorn.run(api_service.app, host="0.0.0.0", port=args.api_port)


if __name__ == "__main__":
    main()