#!/usr/bin/env python
"""CLI script for running all Toronto Auto Theft ETL pipelines."""

import argparse
import logging
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[1].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import (  # noqa: E402
    AUTO_THEFT_CONFIG,
    CENSUS_CONFIG,
    GEOSPATIAL_CONFIG,
)
from src.config.logging_config import get_logger  # noqa: E402
from src.etl.auto_theft_pipeline import AutoTheftPipeline  # noqa: E402
from src.etl.census_pipeline import CensusPipeline  # noqa: E402
from src.etl.geospatial_pipeline import GeospatialPipeline  # noqa: E402

logger = get_logger("etl.run_all_pipelines")


def run_all_pipelines(
    run_auto_theft=True, run_census=True, run_geospatial=True, log_level="INFO"
):
    """Run selected ETL pipelines.

    Args:
        run_auto_theft: Whether to run the auto theft pipeline
        run_census: Whether to run the census pipeline
        run_geospatial: Whether to run the geospatial pipeline
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Set log level
    logging.getLogger().setLevel(log_level)

    success_count = 0
    failure_count = 0

    logger.info("Starting ETL pipeline execution")

    # Run auto theft pipeline
    if run_auto_theft:
        try:
            logger.info("Running Auto Theft pipeline...")
            auto_theft_pipeline = AutoTheftPipeline(AUTO_THEFT_CONFIG)
            auto_theft_pipeline.run()
            success_count += 1
            logger.info("Auto Theft pipeline completed successfully")
        except Exception as e:
            failure_count += 1
            logger.error(f"Auto Theft pipeline failed: {e}")

    # Run census pipeline
    if run_census:
        try:
            logger.info("Running Census pipeline...")
            census_pipeline = CensusPipeline(CENSUS_CONFIG)
            census_pipeline.run()
            success_count += 1
            logger.info("Census pipeline completed successfully")
        except Exception as e:
            failure_count += 1
            logger.error(f"Census pipeline failed: {e}")

    # Run geospatial pipeline
    if run_geospatial:
        try:
            logger.info("Running Geospatial pipeline...")
            geospatial_pipeline = GeospatialPipeline(GEOSPATIAL_CONFIG)
            geospatial_pipeline.run()
            success_count += 1
            logger.info("Geospatial pipeline completed successfully")
        except Exception as e:
            failure_count += 1
            logger.error(f"Geospatial pipeline failed: {e}")

    # Summary
    logger.info(
        f"ETL pipeline execution completed: {success_count} succeeded, "
        f"{failure_count} failed"
    )
    return success_count, failure_count


def main():
    """Main entry point for the CLI interface."""
    parser = argparse.ArgumentParser(
        description="Toronto Auto Theft Data - All ETL Pipelines",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--auto-theft-only",
        action="store_true",
        help="Run only the auto theft ETL pipeline",
    )
    parser.add_argument(
        "--census-only",
        action="store_true",
        help="Run only the census ETL pipeline",
    )
    parser.add_argument(
        "--geospatial-only",
        action="store_true",
        help="Run only the geospatial ETL pipeline",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Determine which pipelines to run
    if args.auto_theft_only:
        run_auto_theft, run_census, run_geospatial = True, False, False
    elif args.census_only:
        run_auto_theft, run_census, run_geospatial = False, True, False
    elif args.geospatial_only:
        run_auto_theft, run_census, run_geospatial = False, False, True
    else:
        # Run all by default
        run_auto_theft, run_census, run_geospatial = True, True, True

    # Run the selected pipelines
    success_count, failure_count = run_all_pipelines(
        run_auto_theft, run_census, run_geospatial, args.log_level
    )

    # Return non-zero exit code if any pipeline failed
    if failure_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
