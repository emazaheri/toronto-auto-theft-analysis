"""Data loading module for Toronto geospatial data (FSAs and Neighbourhoods)."""

import os

import geopandas as gpd
import pandas as pd

from src.config.etl_config import GEOSPATIAL_CONFIG
from src.config.logging_config import get_logger
from src.utils.error_handling import retry

logger = get_logger(__name__)


class GeospatialLoader:
    """Loader class for Toronto geospatial data.

    This class is responsible for saving processed FSA-neighbourhood overlap data
    to parquet format in the processed data directory.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the loader with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or GEOSPATIAL_CONFIG
        self.output_path = self.config["output_path"]

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        logger.info("GeospatialLoader initialized.")
        logger.info(f"Output path: {self.output_path}")

    @retry(IOError, tries=3, delay=2.0)
    def load_data(self, df: gpd.GeoDataFrame) -> None:
        """Save the processed GeoDataFrame to parquet format.

        Args:
            df: Processed GeoDataFrame to save

        Raises:
            IOError: If there's an error writing the file
        """
        logger.info(f"Saving {len(df)} rows to {self.output_path}")

        try:
            # Convert to pandas DataFrame (dropping geometry)
            # if it's a GeoDataFrame and has a geometry column
            if isinstance(df, gpd.GeoDataFrame) and "geometry" in df.columns:
                # ["AREA_LONG_CODE", ...]' was always true and thus redundant.
                df = pd.DataFrame(df[["AREA_LONG_CODE", "CFSAUID", "overlap_percent"]])
                logger.info("Converted GeoDataFrame to DataFrame, dropping geometry")
            elif isinstance(df, gpd.GeoDataFrame):
                df = pd.DataFrame(df[["AREA_LONG_CODE", "CFSAUID", "overlap_percent"]])
                logger.info("Converted GeoDataFrame to DataFrame (standard columns).")

            # Save to parquet format
            df.to_parquet(self.output_path, index=False, compression="snappy")
            logger.info(f"Successfully saved data to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {self.output_path}: {e}")
            raise
