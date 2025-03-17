"""
Main entry point for MLP pipeline
"""

import os
from datetime import datetime

from pipeline.config.run_configuration import RunConfiguration
from pipeline.pipeline import MLPPipeline


def main():
    as_of_date = datetime(2025, 3, 10)  # Default from notebook
    working_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the input, output, and database paths relative to the script directory
    input_dir = os.path.join(working_dir, 'data', 'input')
    output_dir = os.path.join(working_dir, 'data', 'output')
    db_path = os.path.join(working_dir, 'data', 'sec_adv.db')

    # Create directories
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    config = RunConfiguration(
        input_dir=input_dir,
        output_dir=output_dir,
        firm_crd_numbers=[160882, 160021, 317731],
        db_path=db_path,
        raise_error=False,
        verbose=True
    )

    pipeline = MLPPipeline(config)
    pipeline.run(as_of_date)


main()
