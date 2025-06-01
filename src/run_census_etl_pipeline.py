#!/usr/bin/env python
"""CLI script for running the Canada Census 2021 ETL pipeline."""

import argparse
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[1].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import CENSUS_CONFIG  # noqa: E402
from src.etl.census_pipeline import CensusPipeline  # noqa: E402


def main():
    """Main entry point for the CLI interface."""
    parser = argparse.ArgumentParser(
        description="Canada Census 2021 Data ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--geo-input",
        help="Path to geographic index CSV file (overrides config)",
        default=None,
    )
    parser.add_argument(
        "--data-input",
        help="Path to census data CSV file (overrides config)",
        default=None,
    )
    parser.add_argument(
        "--output",
        help="Path for output Parquet file (overrides config)",
        default=None,
    )
    parser.add_argument(
        "--max-level",
        type=int,
        help="Maximum characteristic hierarchy level to include (overrides default)",
        default=None,
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Create custom config with any overridden paths
    config = CENSUS_CONFIG.copy()

    if args.geo_input:
        config["geo_input_path"] = Path(args.geo_input)

    if args.data_input:
        config["data_input_path"] = Path(args.data_input)

    if args.output:
        config["output_path"] = Path(args.output)

    if args.max_level is not None:
        config["max_characteristic_level"] = args.max_level

    # Run the pipeline
    pipeline = CensusPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
