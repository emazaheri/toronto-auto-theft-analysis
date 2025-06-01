#!/usr/bin/env python
"""CLI script for running the Toronto Auto Theft ETL pipeline."""

import argparse
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[1].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import AUTO_THEFT_CONFIG  # noqa: E402
from src.etl.auto_theft_pipeline import AutoTheftPipeline  # noqa: E402


def main():
    """Main entry point for the CLI interface."""
    parser = argparse.ArgumentParser(
        description="Toronto Auto Theft Data ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", help="Path to input CSV file (overrides config)", default=None
    )
    parser.add_argument(
        "--output", help="Path for output Parquet file (overrides config)", default=None
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Create custom config with any overridden paths
    config = AUTO_THEFT_CONFIG.copy()

    if args.input:
        config["input_path"] = Path(args.input)

    if args.output:
        config["output_path"] = Path(args.output)

    # Run the pipeline
    pipeline = AutoTheftPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
