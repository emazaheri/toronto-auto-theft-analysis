"""ETL pipeline for Toronto geospatial data (FSAs and Neighbourhoods).

This module orchestrates the ETL process for Toronto geospatial data:
1. Extract geospatial data from GeoJSON files in raw data directory
2. Transform data by calculating spatial intersections and overlap percentages
3. Load processed data to parquet files in processed data directory
"""

import argparse
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[2].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import GEOSPATIAL_CONFIG  # noqa: E402
from src.config.logging_config import get_logger  # noqa: E402
from src.etl.extractors.geospatial_extractor import GeospatialExtractor  # noqa: E402
from src.etl.loaders.geospatial_loader import GeospatialLoader  # noqa: E402
from src.etl.transformers.geospatial_transformer import (  # noqa: E402
    GeospatialTransformer,  # noqa: E402
)
from src.utils.etl_metrics import ETLMetrics  # noqa: E402

logger = get_logger("etl.geospatial_pipeline")


class GeospatialPipeline:
    """Pipeline class for processing Toronto geospatial data.

    This class orchestrates the entire ETL process by sequentially running
    extraction, transformation, and loading steps for geospatial data.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the pipeline with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or GEOSPATIAL_CONFIG
        self.extractor = GeospatialExtractor(self.config)
        self.transformer = GeospatialTransformer(self.config)
        self.loader = GeospatialLoader(self.config)

        logger.info("GeospatialPipeline initialized")

    def run(self) -> None:
        """Run the complete ETL pipeline.

        Executes extraction, transformation, and loading steps sequentially,
        with timing and logging for each step.
        """
        metrics = ETLMetrics("geospatial_data")
        logger.info("Starting geospatial data ETL pipeline")

        try:
            # Extract data
            metrics.start_stage("extract")
            raw_data = self.extractor.extract_data()
            extract_duration = metrics.end_stage("extract")
            # Count total records
            hood_count = len(raw_data[0])
            fsa_count = len(raw_data[1])
            total_count = hood_count + fsa_count
            metrics.record_row_count("extract", total_count)
            logger.info(f"Data extraction completed in {extract_duration:.2f} seconds")
            logger.info(f"Extracted {hood_count} neighbourhoods and {fsa_count} FSAs")

            # Transform data
            metrics.start_stage("transform")
            transformed_data = self.transformer.transform_data(raw_data)
            transform_duration = metrics.end_stage("transform")
            metrics.record_row_count("transform", len(transformed_data))
            logger.info(
                f"Data transformation completed in {transform_duration:.2f} seconds"
            )
            logger.info(
                f"Generated {len(transformed_data)} " f"FSA-neighbourhood intersections"
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
        description="Run the Toronto geospatial ETL pipeline"
    )
    parser.add_argument(
        "--hood-input",
        type=str,
        help="Input file path for neighbourhoods (overrides default)",
    )
    parser.add_argument(
        "--fsa-input", type=str, help="Input file path for FSAs (overrides default)"
    )
    parser.add_argument(
        "--output", type=str, help="Output file path (overrides default)"
    )
    parser.add_argument(
        "--crs",
        type=str,
        help="Coordinate Reference System (CRS) for spatial calculations",
        default="EPSG:3347",
    )
    parser.add_argument(
        "--min-overlap",
        type=float,
        help="Minimum overlap percentage threshold",
        default=0.001,
    )
    return parser.parse_args()


def main():
    """Main entry point for the ETL pipeline."""
    args = parse_args()

    # Create custom config with any overridden paths
    config = GEOSPATIAL_CONFIG.copy()

    if args.hood_input:
        config["hood_input_path"] = Path(args.hood_input)

    if args.fsa_input:
        config["fsa_input_path"] = Path(args.fsa_input)

    if args.output:
        config["output_path"] = Path(args.output)

    if args.crs:
        config["crs"] = args.crs

    if args.min_overlap:
        config["min_overlap_percent"] = args.min_overlap

    # Run the pipeline
    pipeline = GeospatialPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
