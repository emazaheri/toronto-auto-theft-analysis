"""Data extraction module for Canada Census 2021 data."""

import os

import pandas as pd

from src.config.etl_config import CENSUS_CONFIG
from src.config.logging_config import get_logger
from src.utils.error_handling import retry

logger = get_logger(__name__)


class CensusExtractor:
    """Extractor class for Canada Census 2021 data.

    This class is responsible for extracting data from raw CSV files,
    focusing specifically on Toronto Forward Sortation Areas (FSAs).
    It uses a two-step process where first the geographic index file is loaded
    to determine which rows to extract from the main census file.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the extractor with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or CENSUS_CONFIG
        self.geo_input_path = self.config["geo_input_path"]
        self.data_input_path = self.config["data_input_path"]
        self.fsa_prefix = self.config["fsa_prefix"]
        self.encoding = self.config["encoding"]

        logger.info(
            f"CensusExtractor initialized with paths: {self.geo_input_path}, "
            f"{self.data_input_path}"
        )

    @retry((IOError, pd.errors.EmptyDataError), tries=3, delay=2.0)
    def extract_geo_data(self) -> pd.DataFrame:
        """Extract geographic index data for Toronto FSAs.

        Returns:
            DataFrame with geographic index information for Toronto FSAs

        Raises:
            FileNotFoundError: If the input file doesn't exist
            IOError: If there's an error reading the file
            pd.errors.EmptyDataError: If the file is empty
        """
        logger.info(f"Extracting geographic index data from {self.geo_input_path}")

        if not os.path.exists(self.geo_input_path):
            logger.error(f"Geographic index file not found: {self.geo_input_path}")
            raise FileNotFoundError(
                f"Geographic index file not found: {self.geo_input_path}"
            )

        try:
            # Read the geographic index file
            df_geo = pd.read_csv(self.geo_input_path)

            # Filter for Toronto FSAs (those starting with 'M')
            df_geo_filtered = df_geo[df_geo["Geo Name"].str.startswith(self.fsa_prefix)]

            if df_geo_filtered.empty:
                logger.warning(f"No Toronto FSAs found with prefix '{self.fsa_prefix}'")
            else:
                logger.info(f"Extracted {len(df_geo_filtered)} Toronto FSAs")

            return df_geo_filtered

        except pd.errors.EmptyDataError:
            logger.error(f"Empty geographic index file: {self.geo_input_path}")
            raise
        except OSError as e:
            logger.error(f"IOError while reading {self.geo_input_path}: {e}")
            raise
        except Exception as e:
            logger.exception(
                f"Unexpected error while extracting geographic index data: {e}"
            )
            raise

    @retry((IOError, pd.errors.EmptyDataError), tries=3, delay=2.0)
    def extract_census_data(self, df_geo: pd.DataFrame) -> pd.DataFrame:
        """Extract census data for Toronto FSAs.

        Uses the geographic index data to extract only the relevant rows
        from the main census file, significantly reducing memory usage.

        Args:
            df_geo: DataFrame with geographic index information for Toronto FSAs

        Returns:
            DataFrame with census data for Toronto FSAs

        Raises:
            FileNotFoundError: If the input file doesn't exist
            IOError: If there's an error reading the file
            pd.errors.EmptyDataError: If the file is empty
        """
        logger.info(f"Extracting census data from {self.data_input_path}")

        if not os.path.exists(self.data_input_path):
            logger.error(f"Census data file not found: {self.data_input_path}")
            raise FileNotFoundError(
                f"Census data file not found: {self.data_input_path}"
            )

        try:
            # Calculate skiprows and nrows based on the geographic index data
            nskiprows = df_geo["Line Number"].iloc[0]
            nrows = df_geo["Line Number"].iloc[-1] - nskiprows + 1

            logger.info(
                f"Extracting census data rows {nskiprows + 1} to {nskiprows + nrows}"
            )

            # Get original column names to map SYMBOL columns correctly
            original_columns = pd.read_csv(
                self.data_input_path, nrows=0, encoding=self.encoding
            ).columns.tolist()

            new_columns = []
            column_dtypes = self.config["column_dtypes"].copy()

            # Rename SYMBOL columns to associate them with their data columns
            for col in original_columns:
                if col == "SYMBOL":
                    new_columns.append("C1_SYMBOL")
                    column_dtypes["C1_SYMBOL"] = "category"
                elif col.startswith("SYMBOL."):
                    idx = int(col.split(".")[1])
                    new_columns.append(f"C{idx + 1}_SYMBOL")
                    column_dtypes[f"C{idx + 1}_SYMBOL"] = "category"
                else:
                    new_columns.append(col)

            # Read the census data with optimized parameters
            df_census = pd.read_csv(
                self.data_input_path,
                header=0,
                encoding=self.encoding,
                skiprows=range(1, nskiprows + 1),
                nrows=nrows,
                names=new_columns,
                usecols=lambda x: x not in self.config["columns_to_drop"],
                dtype=column_dtypes,
            )

            logger.info(
                f"Extracted {len(df_census)} rows and "
                f"{len(df_census.columns)} columns of census data"
            )
            return df_census

        except pd.errors.EmptyDataError:
            logger.error(f"Empty census data file: {self.data_input_path}")
            raise
        except OSError as e:
            logger.error(f"IOError while reading {self.data_input_path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while extracting census data: {e}")
            raise

    def extract_data(self) -> pd.DataFrame:
        """Extract both geographic and census data in one operation.

        Returns:
            DataFrame with census data for Toronto FSAs
        """
        # First extract the geographic index data
        df_geo = self.extract_geo_data()

        # Then use it to extract the census data
        df_census = self.extract_census_data(df_geo)

        return df_census
