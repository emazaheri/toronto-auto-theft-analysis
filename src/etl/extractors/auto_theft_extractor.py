"""Data extraction module for Toronto auto theft data."""

import os

import pandas as pd

from src.config.etl_config import AUTO_THEFT_CONFIG
from src.config.logging_config import get_logger
from src.utils.error_handling import retry

logger = get_logger(__name__)


class AutoTheftExtractor:
    """Extractor class for Toronto auto theft data.

    This class is responsible for extracting data from raw CSV files,
    applying initial data type conversions, and handling missing values.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the extractor with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or AUTO_THEFT_CONFIG
        self.input_path = self.config["input_path"]
        self.date_columns = self.config["date_columns"]
        self.column_dtypes = self.config["column_dtypes"]
        self.columns_to_drop = self.config["columns_to_drop"]
        self.na_values = self.config["na_values"]
        self.converters = self.config["converters"]

        logger.info(
            f"AutoTheftExtractor initialized with input path: {self.input_path}"
        )

    @retry((IOError, pd.errors.EmptyDataError), tries=3, delay=2.0)
    def extract_data(self) -> pd.DataFrame:
        """Extract auto theft data from CSV file.

        Returns:
            DataFrame with extracted auto theft data

        Raises:
            FileNotFoundError: If the input file doesn't exist
            IOError: If there's an error reading the file
            pd.errors.EmptyDataError: If the file is empty
        """
        logger.info(f"Extracting data from {self.input_path}")

        if not os.path.exists(self.input_path):
            logger.error(f"Input file not found: {self.input_path}")
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        try:
            df = pd.read_csv(
                self.input_path,
                parse_dates=self.date_columns,
                dtype=self.column_dtypes,
                usecols=lambda col: col not in self.columns_to_drop,
                na_values=self.na_values,
                converters=self.converters,
            )

            # Convert DOW columns to category type
            if "REPORT_DOW" in df.columns:
                df["REPORT_DOW"] = df["REPORT_DOW"].astype("category")
            if "OCC_DOW" in df.columns:
                df["OCC_DOW"] = df["OCC_DOW"].astype("category")

            logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")
            return df

        except pd.errors.EmptyDataError:
            logger.error(f"Empty file: {self.input_path}")
            raise
        except OSError as e:
            logger.error(f"IOError while reading {self.input_path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while extracting data: {e}")
            raise
