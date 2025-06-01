"""Data transformation module for Toronto auto theft data."""

import pandas as pd

from src.config.etl_config import AUTO_THEFT_CONFIG
from src.config.logging_config import get_logger
from src.utils.validation import (
    validate_coordinates,
    validate_date_logic,
    validate_date_range,
)

logger = get_logger(__name__)


class AutoTheftTransformer:
    """Transformer class for Toronto auto theft data.

    This class is responsible for cleaning, transforming, and enriching
    the auto theft data, including deduplication, imputation, validation,
    and feature engineering.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the transformer with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or AUTO_THEFT_CONFIG
        self.coord_validation = self.config["coord_validation"]
        self.valid_years = self.config["valid_years"]
        self.hour_bins = self.config["hour_bins"]
        self.hour_labels = self.config["hour_labels"]
        self.season_map = self.config["season_map"]
        self.season_order = self.config["season_order"]
        self.weekend_days = self.config["weekend_days"]

        logger.info("AutoTheftTransformer initialized")

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform auto theft data through a series of cleaning and enrichment steps.

        Args:
            df: DataFrame with raw auto theft data

        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info(f"Starting data transformation on {len(df)} rows")

        # Apply transformation steps sequentially
        df = self._fix_timestamps(df)
        df = self._remove_duplicates(df)
        df = self._drop_null_occurrence_dates(df)
        df = self._drop_rows_without_geospatial_info(df)
        df = self._impute_missing_hood_with_division(df)
        df = self._impute_missing_coordinates(df)

        # Data validation and cleaning steps
        df = self._validate_and_filter_data(df)

        # Feature engineering
        df = self._add_time_bin_feature(df)
        df = self._add_season_feature(df)
        df = self._add_weekend_feature(df)

        logger.info(f"Completed transformation, resulting in {len(df)} rows")
        return df

    def _fix_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix the timestamps in date columns using hour columns.

        Args:
            df: DataFrame with date columns

        Returns:
            DataFrame with corrected timestamps
        """
        logger.info("Fixing timestamps in date columns")

        df_copy = df.copy()

        # Fix REPORT_DATE timestamp using REPORT_HOUR
        df_copy["REPORT_DATE"] = df_copy.apply(
            lambda x: x["REPORT_DATE"].replace(
                hour=int(x["REPORT_HOUR"]), minute=0, second=0
            )
            if pd.notnull(x["REPORT_HOUR"])
            else x["REPORT_DATE"],
            axis=1,
        )

        # Fix OCC_DATE timestamp using OCC_HOUR
        df_copy["OCC_DATE"] = df_copy.apply(
            lambda x: x["OCC_DATE"].replace(hour=int(x["OCC_HOUR"]), minute=0, second=0)
            if pd.notnull(x["OCC_HOUR"])
            else x["OCC_DATE"],
            axis=1,
        )

        return df_copy

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows from the DataFrame.

        Args:
            df: DataFrame with possible duplicates

        Returns:
            DataFrame with duplicates removed
        """
        initial_count = len(df)
        df_dedup = df.drop_duplicates(keep="first").reset_index(drop=True)

        # Check for duplicate EVENT_UNIQUE_IDs with different data
        unique_id_count = df_dedup["EVENT_UNIQUE_ID"].nunique()
        if len(df_dedup) != unique_id_count:
            logger.warning(
                f"Found {len(df_dedup) - unique_id_count} rows with duplicate "
                "EVENT_UNIQUE_ID but different data"
            )

        logger.info(f"Removed {initial_count - len(df_dedup)} duplicate rows")
        return df_dedup

    def _drop_null_occurrence_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows with null occurrence date components.

        Args:
            df: DataFrame with possibly null occurrence dates

        Returns:
            DataFrame with rows having null occurrence dates removed
        """
        initial_count = len(df)
        df_clean = df.dropna(
            subset=["OCC_YEAR", "OCC_DAY", "OCC_DOY"],
            how="all",
        ).reset_index(drop=True)

        logger.info(
            f"Removed {initial_count - len(df_clean)} rows with null occurrence dates"
        )
        return df_clean

    def _drop_rows_without_geospatial_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows that lack all geospatial information.

        Args:
            df: DataFrame with possibly missing geospatial info

        Returns:
            DataFrame with rows lacking all geospatial info removed
        """
        initial_count = len(df)

        # Identify rows with no geospatial information
        mask = (
            df["LAT_WGS84"].isna()
            & df["LONG_WGS84"].isna()
            & df["HOOD_158"].isna()
            & df["DIVISION"].isna()
        )

        df_clean = df[~mask].reset_index(drop=True)

        logger.info(
            f"Removed {initial_count - len(df_clean)} rows lacking all geospatial info"
        )
        return df_clean

    def _impute_missing_hood_with_division(self, df: pd.DataFrame) -> pd.DataFrame:
        """Impute missing HOOD_158 and NEIGHBOURHOOD_158 using DIVISION.

        Args:
            df: DataFrame with possibly missing neighborhood info

        Returns:
            DataFrame with imputed neighborhood information
        """
        df_imputed = df.copy()

        # Identify rows with missing hood but valid division
        mask = df_imputed["HOOD_158"].isna() & df_imputed["DIVISION"].notna()
        missing_hood_count = mask.sum()

        if missing_hood_count == 0:
            logger.info("No missing neighborhoods to impute")
            return df_imputed

        logger.info(
            f"Imputing {missing_hood_count} missing neighborhoods using division"
        )

        # Build a lookup: DIVISION -> modal HOOD_158
        hood_mode_by_div = (
            df_imputed[df_imputed["HOOD_158"].notna()]  # only rows with a known hood
            .groupby("DIVISION")["HOOD_158"]
            .agg(
                lambda s: s.mode().iat[0] if not s.empty and len(s.mode()) > 0 else None
            )
        )

        # Build a lookup to get the neighbourhood name as well
        name_lookup = (
            df_imputed[["HOOD_158", "NEIGHBOURHOOD_158"]]
            .dropna()
            .drop_duplicates()
            .set_index("HOOD_158")["NEIGHBOURHOOD_158"]
        )

        # Apply to rows whose hood is missing but division known
        df_imputed.loc[mask, "HOOD_158"] = df_imputed.loc[mask, "DIVISION"].map(
            hood_mode_by_div
        )
        df_imputed.loc[mask, "NEIGHBOURHOOD_158"] = df_imputed.loc[
            mask, "HOOD_158"
        ].map(name_lookup)

        # Check if imputation was successful
        still_missing = df_imputed.loc[mask, "HOOD_158"].isna().sum()
        if still_missing > 0:
            logger.warning(
                f"Could not impute {still_missing} neighborhoods due to "
                "missing lookup data"
            )

        return df_imputed

    def _impute_missing_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Impute missing LAT_WGS84 and LONG_WGS84 using HOOD_158 centroids.

        Args:
            df: DataFrame with possibly missing coordinates

        Returns:
            DataFrame with imputed coordinates
        """
        df_imputed = df.copy()

        # Check for missing coordinates
        missing_coord_mask = (
            df_imputed["LAT_WGS84"].isna() | df_imputed["LONG_WGS84"].isna()
        )
        missing_coord_count = missing_coord_mask.sum()

        if missing_coord_count == 0:
            logger.info("No missing coordinates to impute")
            return df_imputed

        logger.info(
            f"Imputing {missing_coord_count} missing coordinates "
            "using neighborhood centroids"
        )

        # Calculate centroids for each neighbourhood
        centroids = df_imputed.groupby("HOOD_158", observed=False)[
            ["LAT_WGS84", "LONG_WGS84"]
        ].median()

        # Fill missing coordinates with the centroid of the same neighbourhood
        df_with_centroids = (
            df_imputed.set_index("HOOD_158")
            .join(centroids, rsuffix="_cent")
            .reset_index()
        )

        # Apply imputation where needed
        mask_lat = df_with_centroids["LAT_WGS84"].isna()
        mask_long = df_with_centroids["LONG_WGS84"].isna()

        df_with_centroids.loc[mask_lat, "LAT_WGS84"] = df_with_centroids.loc[
            mask_lat, "LAT_WGS84_cent"
        ]
        df_with_centroids.loc[mask_long, "LONG_WGS84"] = df_with_centroids.loc[
            mask_long, "LONG_WGS84_cent"
        ]

        # Remove the temporary centroid columns
        df_imputed = df_with_centroids.drop(
            columns=["LAT_WGS84_cent", "LONG_WGS84_cent"]
        )

        # Check if there are still missing coordinates
        still_missing = (
            df_imputed["LAT_WGS84"].isna().sum() + df_imputed["LONG_WGS84"].isna().sum()
        )
        if still_missing > 0:
            logger.warning(f"Could not impute {still_missing} coordinate values")

        return df_imputed

    def _validate_and_filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and filter data based on various criteria.

        Args:
            df: DataFrame to validate and filter

        Returns:
            Validated and filtered DataFrame
        """
        logger.info("Validating and filtering data")

        df_valid = df.copy()
        initial_count = len(df_valid)

        # Validate coordinates are within Toronto boundaries
        valid_df, invalid_coords = validate_coordinates(
            df_valid,
            "LAT_WGS84",
            "LONG_WGS84",
            self.coord_validation["lat_min"],
            self.coord_validation["lat_max"],
            self.coord_validation["long_min"],
            self.coord_validation["long_max"],
        )

        if len(invalid_coords) > 0:
            logger.warning(
                f"Found {len(invalid_coords)} rows with coordinates "
                "outside Toronto boundaries"
            )
            df_valid = valid_df

        # Validate occurrence date is not later than report date
        valid_df, invalid_dates = validate_date_logic(
            df_valid, "OCC_DATE", "REPORT_DATE"
        )

        if len(invalid_dates) > 0:
            logger.warning(
                f"Found {len(invalid_dates)} rows with occurrence date "
                "later than report date"
            )
            df_valid = valid_df

        # Validate occurrence date is within expected range (2013-2024)
        valid_df, invalid_years = validate_date_range(
            df_valid, "OCC_DATE", "2013-01-01", "2024-12-31"
        )

        if len(invalid_years) > 0:
            logger.warning(
                f"Found {len(invalid_years)} rows with occurrence date "
                "outside valid range (2013-2024)"
            )
            df_valid = valid_df

        logger.info(
            f"Removed {initial_count - len(df_valid)} rows that failed validation"
        )
        return df_valid.reset_index(drop=True)

    def _add_time_bin_feature(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add time bin feature based on occurrence hour.

        Args:
            df: DataFrame with OCC_HOUR column

        Returns:
            DataFrame with added OCC_TIME_BIN column
        """
        logger.info("Adding OCC_TIME_BIN feature")

        df_with_feature = df.copy()

        df_with_feature["OCC_TIME_BIN"] = pd.cut(
            df_with_feature["OCC_HOUR"],
            bins=self.hour_bins,
            labels=self.hour_labels,
            ordered=False,
        )

        return df_with_feature

    def _add_season_feature(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add season feature based on occurrence month.

        Args:
            df: DataFrame with OCC_MONTH column

        Returns:
            DataFrame with added SEASON column
        """
        logger.info("Adding SEASON feature")

        df_with_feature = df.copy()

        df_with_feature["SEASON"] = df_with_feature["OCC_MONTH"].map(self.season_map)
        df_with_feature["SEASON"] = pd.Categorical(
            df_with_feature["SEASON"], categories=self.season_order, ordered=False
        )

        return df_with_feature

    def _add_weekend_feature(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add weekend indicator feature based on occurrence day of week.

        Args:
            df: DataFrame with OCC_DOW column

        Returns:
            DataFrame with added IS_WEEKEND column
        """
        logger.info("Adding IS_WEEKEND feature")

        df_with_feature = df.copy()

        df_with_feature["IS_WEEKEND"] = df_with_feature["OCC_DOW"].isin(
            self.weekend_days
        )

        return df_with_feature
