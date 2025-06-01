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
    def calculate_row_indices(self) -> tuple[int, int]:
        """Calculate the skiprows and nrows parameters for census data extraction.

        Uses the geographic index data to determine which rows of the main
        census file contain Toronto FSA data, significantly reducing memory usage.

        Returns:
            tuple: (nskiprows, nrows) where:
                - nskiprows: Number of rows to skip from the start of the file
                - nrows: Number of rows to read after skipping

        Raises:
            FileNotFoundError: If the geographic index file doesn't exist
            IOError: If there's an error reading the file
            pd.errors.EmptyDataError: If the file is empty
        """
        logger.info(f"Calculating row indices from {self.geo_input_path}")

        # Default values in case we can't calculate proper ones
        default_skiprows = 0
        default_nrows = 1000000  # Large enough to get all data

        if not os.path.exists(self.geo_input_path):
            logger.error(f"Geographic index file not found: {self.geo_input_path}")
            logger.warning("Using default row indices")
            return default_skiprows, default_nrows

        try:
            # Read the geographic index file
            df_geo = pd.read_csv(self.geo_input_path)

            # Find rows where Geo Name starts with the FSA prefix
            toronto_mask = df_geo["Geo Name"].str.startswith(self.fsa_prefix)

            if not toronto_mask.any():
                logger.warning(f"No FSAs found with prefix '{self.fsa_prefix}'")
                logger.warning("Using default row indices")
                return default_skiprows, default_nrows

            # Find the start line (first Toronto FSA)
            start_line = df_geo[toronto_mask]["Line Number"].min()

            # Find the end line (first FSA after Toronto)
            next_fsa_group_char = chr(ord(self.fsa_prefix[0]) + 1)
            next_fsa_mask = df_geo["Geo Name"].str.startswith(next_fsa_group_char)

            if next_fsa_mask.any():
                # Get the first line number where Geo Name starts with next character
                end_line = df_geo[next_fsa_mask]["Line Number"].min()
            else:
                # If no FSAs after Toronto, use the max line number + 1
                end_line = df_geo[toronto_mask]["Line Number"].max() + 1

            # Calculate skiprows and nrows
            nskiprows = start_line - 1  # skip header and lines before Toronto
            nrows = end_line - start_line  # number of Toronto rows

            logger.info(
                f"Row indices: start={start_line}, end={end_line}, "
                f"skiprows={nskiprows}, nrows={nrows}"
            )

            return nskiprows, nrows

        except pd.errors.EmptyDataError:
            logger.error(f"Empty geographic index file: {self.geo_input_path}")
            logger.warning("Using default row indices")
            return default_skiprows, default_nrows
        except OSError as e:
            logger.error(f"IOError while reading {self.geo_input_path}: {e}")
            logger.warning("Using default row indices")
            return default_skiprows, default_nrows
        except Exception as e:
            logger.exception(f"Unexpected error while calculating row indices: {e}")
            logger.warning("Using default row indices")
            return default_skiprows, default_nrows

    @retry((IOError, pd.errors.EmptyDataError), tries=3, delay=2.0)
    def extract_census_data(self, nskiprows: int, nrows: int) -> pd.DataFrame:
        """Extract census data for Toronto FSAs.

        Uses the provided row indices to extract only the relevant rows
        from the main census file, significantly reducing memory usage.

        Args:
            nskiprows: Number of rows to skip from the start of the file
            nrows: Number of rows to read after skipping

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
                skiprows=range(1, nskiprows),
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
        """Extract census data in one operation.

        Returns:
            DataFrame with census data for Toronto FSAs
        """
        # First calculate the row indices
        nskiprows, nrows = self.calculate_row_indices()

        # Then extract the census data using those indices
        df_census = self.extract_census_data(nskiprows, nrows)

        return df_census
