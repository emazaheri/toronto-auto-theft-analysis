"""Data transformation module for Canada Census 2021 data."""

import pandas as pd

from src.config.etl_config import CENSUS_CONFIG
from src.config.logging_config import get_logger

logger = get_logger(__name__)


class CensusTransformer:
    """Transformer class for Canada Census 2021 data.

    This class is responsible for cleaning and transforming census data,
    including handling missing values, optimizing data types, adding derived
    features, and filtering data based on specified criteria.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the transformer with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or CENSUS_CONFIG
        self.max_characteristic_level = self.config["max_characteristic_level"]

        logger.info("CensusTransformer initialized")

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform census data through a series of cleaning and enrichment steps.

        Args:
            df: DataFrame with raw census data

        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info(f"Starting census data transformation on {len(df)} rows")

        # Apply transformation steps sequentially
        df = self._check_identify_values(df)
        df = self._extract_characteristic_hierarchy(df)
        df = self._filter_by_characteristic_level(df)
        df = self._optimize_data_types(df)

        logger.info(f"Completed transformation, resulting in {len(df)} rows")
        return df

    def _check_identify_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check if ALT_GEO_CODE and GEO_NAME are the same.

        This step helps validate that our primary geographic identifier is consistent.

        Args:
            df: DataFrame with census data

        Returns:
            Validated DataFrame
        """
        logger.info("Checking geographic identifier consistency")

        df_copy = df.copy()

        # Check if ALT_GEO_CODE and GEO_NAME are identical
        if "GEO_NAME" in df_copy.columns:
            df_copy["ALT_GEO_CODE"] = df_copy["ALT_GEO_CODE"].astype(str)
            df_copy["GEO_NAME"] = df_copy["GEO_NAME"].astype(str)

            is_identical = (df_copy["ALT_GEO_CODE"] == df_copy["GEO_NAME"]).all()

            if is_identical:
                logger.info("ALT_GEO_CODE and GEO_NAME are identical")
                # GEO_NAME is already marked for dropping in the config
            else:
                logger.warning("ALT_GEO_CODE and GEO_NAME differ in some rows")

        return df_copy

    def _extract_characteristic_hierarchy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract hierarchy level from CHARACTERISTIC_NAME based on leading spaces.

        The census data uses leading spaces in CHARACTERISTIC_NAME to indicate
        hierarchical relationships. This function quantifies that hierarchy.

        Args:
            df: DataFrame with census data

        Returns:
            DataFrame with added CHARACTERISTIC_LEVEL column
        """
        logger.info("Extracting characteristic hierarchy")

        df_copy = df.copy()

        # Extract hierarchy level from leading spaces
        df_copy["CHARACTERISTIC_LEVEL"] = df_copy["CHARACTERISTIC_NAME"].apply(
            lambda x: int((len(x) - len(x.lstrip(" "))) / 2)
        )

        # Clean up the CHARACTERISTIC_NAME by removing leading/trailing spaces
        df_copy["CHARACTERISTIC_NAME"] = df_copy["CHARACTERISTIC_NAME"].str.strip()

        logger.info(
            f"Extracted {df_copy['CHARACTERISTIC_LEVEL'].nunique()} hierarchy levels"
        )
        return df_copy

    def _filter_by_characteristic_level(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter characteristics based on their hierarchy level.

        Higher-level characteristics (with lower CHARACTERISTIC_LEVEL values)
        are more general and often more useful for high-level analysis.

        Args:
            df: DataFrame with census data including CHARACTERISTIC_LEVEL

        Returns:
            Filtered DataFrame
        """
        logger.info(
            f"Filtering characteristics with level < {self.max_characteristic_level}"
        )

        df_filtered = df[df["CHARACTERISTIC_LEVEL"] < self.max_characteristic_level]

        rows_removed = len(df) - len(df_filtered)
        logger.info(f"Removed {rows_removed} rows with deeper hierarchy levels")

        return df_filtered

    def _optimize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for better memory usage and performance.

        Args:
            df: DataFrame with census data

        Returns:
            DataFrame with optimized data types
        """
        logger.info("Optimizing data types")

        df_optimized = df.copy()

        # Convert CHARACTERISTIC_NAME to category
        if "CHARACTERISTIC_NAME" in df_optimized.columns:
            df_optimized["CHARACTERISTIC_NAME"] = df_optimized[
                "CHARACTERISTIC_NAME"
            ].astype("category")

        # Ensure ALT_GEO_CODE is the right type for joining with other datasets
        if "ALT_GEO_CODE" in df_optimized.columns:
            df_optimized["ALT_GEO_CODE"] = (
                df_optimized["ALT_GEO_CODE"].astype(str).str.strip()
            )

        # Find numeric data columns (those starting with C and not ending with SYMBOL)
        # data_columns = [
        #     col
        #     for col in df_optimized.columns
        #     if col.startswith("C") and not col.endswith("SYMBOL")
        # ]

        # For columns with low cardinality, convert to category
        for col in df_optimized.select_dtypes(include=["object"]).columns:
            unique_count = df_optimized[col].nunique()
            if unique_count < 100:  # Threshold for considering categorical
                df_optimized[col] = df_optimized[col].astype("category")

        # Log memory usage
        memory_usage_mb = df_optimized.memory_usage(deep=True).sum() / 1e6
        logger.info(f"Optimized DataFrame size: {memory_usage_mb:.2f} MB")

        return df_optimized
