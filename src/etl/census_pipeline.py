"""ETL pipeline for Canada Census 2021 data.

This module orchestrates the ETL process for Canada Census 2021 data:
1. Extract data from CSV files in raw data directory
2. Transform data by cleaning, validating, and enhancing it
3. Load processed data to parquet files in processed data directory
"""

import argparse
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[2].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import CENSUS_CONFIG  # noqa: E402
from src.config.logging_config import get_logger  # noqa: E402
from src.etl.extractors.census_extractor import CensusExtractor  # noqa: E402
from src.etl.loaders.census_loader import CensusLoader  # noqa: E402
from src.etl.transformers.census_transformer import CensusTransformer  # noqa: E402
from src.utils.etl_metrics import ETLMetrics  # noqa: E402

logger = get_logger("etl.census_pipeline")


class CensusPipeline:
    """Pipeline class for processing Canada Census 2021 data.

    This class orchestrates the entire ETL process by sequentially running
    extraction, transformation, and loading steps.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the pipeline with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or CENSUS_CONFIG
        self.extractor = CensusExtractor(self.config)
        self.transformer = CensusTransformer(self.config)
        self.loader = CensusLoader(self.config)

        logger.info("CensusPipeline initialized")

    def run(self) -> None:
        """Run the complete ETL pipeline.

        Executes extraction, transformation, and loading steps sequentially,
        with timing and logging for each step.
        """
        metrics = ETLMetrics("census_data")
        logger.info("Starting census data ETL pipeline")

        try:
            # Extract data
            metrics.start_stage("extract")
            raw_data = self.extractor.extract_data()
            extract_duration = metrics.end_stage("extract")
            metrics.record_row_count("extract", len(raw_data))
            metrics.record_memory_usage(
                "extract", raw_data.memory_usage(deep=True).sum() / 1e6
            )
            logger.info(f"Data extraction completed in {extract_duration:.2f} seconds")

            # Transform data
            metrics.start_stage("transform")
            transformed_data = self.transformer.transform_data(raw_data)
            transform_duration = metrics.end_stage("transform")
            metrics.record_row_count("transform", len(transformed_data))
            metrics.record_memory_usage(
                "transform", transformed_data.memory_usage(deep=True).sum() / 1e6
            )
            logger.info(
                f"Data transformation completed in {transform_duration:.2f} seconds"
            )

            # Load data
            metrics.start_stage("load")
            self.loader.load_data(transformed_data)
            load_duration = metrics.end_stage("load")
            metrics.record_row_count("final", len(transformed_data))
            logger.info(f"Data loading completed in {load_duration:.2f} seconds")

            # Finalize and save metrics
            metrics.finalize()
            metrics_file = metrics.save()

            logger.info(
                f"Pipeline completed successfully in "
                f"{metrics.metrics['total_duration']:.2f} seconds"
            )
            logger.info(f"Metrics saved to {metrics_file}")
            logger.info("\n" + metrics.summary())

        except Exception as e:
            metrics.save("failed")
            logger.exception(f"Pipeline failed: {e}")
            raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the Canada Census 2021 ETL pipeline"
    )
    parser.add_argument(
        "--geo-input",
        type=str,
        help="Geographic index input file path (overrides default)",
    )
    parser.add_argument(
        "--data-input", type=str, help="Census data input file path (overrides default)"
    )
    parser.add_argument(
        "--output", type=str, help="Output file path (overrides default)"
    )
    parser.add_argument(
        "--max-level",
        type=int,
        help="Maximum characteristic hierarchy level to include (overrides default)",
    )
    return parser.parse_args()


def main():
    """Main entry point for the ETL pipeline."""
    args = parse_args()

    # Create custom config with any overridden paths
    config = CENSUS_CONFIG.copy()

    if args.geo_input:
        config["geo_input_path"] = Path(args.geo_input)

    if args.data_input:
        config["data_input_path"] = Path(args.data_input)

    if args.output:
        config["output_path"] = Path(args.output)

    if args.max_level:
        config["max_characteristic_level"] = args.max_level

    # Run the pipeline
    pipeline = CensusPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
