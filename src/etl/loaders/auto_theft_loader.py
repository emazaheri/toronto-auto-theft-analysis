"""Data loading module for Toronto auto theft data."""

import os

import pandas as pd

from src.config.etl_config import AUTO_THEFT_CONFIG
from src.config.logging_config import get_logger
from src.utils.error_handling import retry

logger = get_logger(__name__)


class AutoTheftLoader:
    """Loader class for Toronto auto theft data.

    This class is responsible for saving processed auto theft data
    to parquet format in the processed data directory.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the loader with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or AUTO_THEFT_CONFIG
        self.output_path = self.config["output_path"]

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        logger.info(f"AutoTheftLoader initialized with output path: {self.output_path}")

    @retry(IOError, tries=3, delay=2.0)
    def load_data(self, df: pd.DataFrame) -> None:
        """Save the processed DataFrame to parquet format.

        Args:
            df: Processed DataFrame to save

        Raises:
            IOError: If there's an error writing the file
        """
        logger.info(f"Saving {len(df)} rows to {self.output_path}")

        try:
            # Optimize DataFrame for parquet storage
            optimized_df = self._optimize_for_parquet(df)

            # Save to parquet format
            optimized_df.to_parquet(self.output_path, index=False, compression="snappy")

            file_size_mb = os.path.getsize(self.output_path) / (1024 * 1024)
            logger.info(
                f"Successfully saved data to {self.output_path} ({file_size_mb:.2f} MB)"
            )

        except OSError as e:
            logger.error(f"IOError while writing to {self.output_path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while saving data: {e}")
            raise

    def _optimize_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame for parquet storage by adjusting data types.

        Args:
            df: DataFrame to optimize

        Returns:
            Optimized DataFrame
        """
        optimized_df = df.copy()

        # Ensure category columns are properly stored
        category_columns = [
            col
            for col, dtype in optimized_df.dtypes.items()
            if dtype.name == "category"
        ]

        # Convert string columns to categorical if they have low cardinality
        for col in optimized_df.select_dtypes(include=["object"]).columns:
            if col not in category_columns:
                unique_count = optimized_df[col].nunique()
                if unique_count < 100:  # Threshold for considering categorical
                    optimized_df[col] = optimized_df[col].astype("category")
                    logger.debug(f"Converted column '{col}' to category type")

        return optimized_df
