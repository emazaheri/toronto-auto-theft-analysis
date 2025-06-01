#!/usr/bin/env python
"""CLI script for running the Toronto Geospatial ETL pipeline."""

import argparse
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
project_root = Path(__file__).parents[1].absolute()
sys.path.append(str(project_root))

from src.config.etl_config import GEOSPATIAL_CONFIG  # noqa: E402
from src.etl.geospatial_pipeline import GeospatialPipeline  # noqa: E402


def main():
    """Main entry point for the CLI interface."""
    parser = argparse.ArgumentParser(
        description="Toronto Geospatial Data ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--hood-input",
        help="Path to input neighborhoods GeoJSON file (overrides config)",
        default=None,
    )
    parser.add_argument(
        "--fsa-input",
        help="Path to input FSA GeoJSON file (overrides config)",
        default=None,
    )
    parser.add_argument(
        "--output", help="Path for output Parquet file (overrides config)", default=None
    )
    parser.add_argument(
        "--crs",
        help="Coordinate Reference System (CRS) for spatial calculations",
        default="EPSG:3347",
    )
    parser.add_argument(
        "--min-overlap",
        type=float,
        help="Minimum overlap percentage threshold",
        default=0.001,
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

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
