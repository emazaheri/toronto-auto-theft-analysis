"""Utility functions for validation and data quality checks."""

import pandas as pd


def validate_coordinates(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate geographic coordinates are within expected boundaries.

    Args:
        df: DataFrame containing coordinates
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        lat_min: Minimum valid latitude
        lat_max: Maximum valid latitude
        lon_min: Minimum valid longitude
        lon_max: Maximum valid longitude

    Returns:
        Tuple of (valid_df, invalid_df)
    """
    mask = (
        (df[lat_col] >= lat_min)
        & (df[lat_col] <= lat_max)
        & (df[lon_col] >= lon_min)
        & (df[lon_col] <= lon_max)
    )

    valid_df = df[mask].copy()
    invalid_df = df[~mask].copy()

    return valid_df, invalid_df


def validate_date_logic(
    df: pd.DataFrame, occurrence_date_col: str, report_date_col: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate occurrence date is not later than report date.

    Args:
        df: DataFrame containing date columns
        occurrence_date_col: Name of occurrence date column
        report_date_col: Name of report date column

    Returns:
        Tuple of (valid_df, invalid_df)
    """
    mask = df[occurrence_date_col] <= df[report_date_col]

    valid_df = df[mask].copy()
    invalid_df = df[~mask].copy()

    return valid_df, invalid_df


def validate_date_range(
    df: pd.DataFrame, date_col: str, min_date: str, max_date: str | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate dates are within expected range.

    Args:
        df: DataFrame containing date column
        date_col: Name of date column to validate
        min_date: Minimum valid date as string (YYYY-MM-DD)
        max_date: Maximum valid date as string (YYYY-MM-DD), defaults to current date

    Returns:
        Tuple of (valid_df, invalid_df)
    """
    mask = df[date_col] >= pd.Timestamp(min_date)

    if max_date:
        mask = mask & (df[date_col] <= pd.Timestamp(max_date))

    valid_df = df[mask].copy()
    invalid_df = df[~mask].copy()

    return valid_df, invalid_df


def check_missing_data(
    df: pd.DataFrame, threshold: float = 0.05, exclude_cols: list[str] | None = None
) -> dict[str, float]:
    """Check for missing data in DataFrame columns.

    Args:
        df: DataFrame to check
        threshold: Maximum acceptable percentage of missing values
        exclude_cols: List of columns to exclude from check

    Returns:
        Dictionary of {column_name: missing_percentage} for columns exceeding threshold
    """
    exclude_cols = exclude_cols or []
    result = {}

    for col in df.columns:
        if col in exclude_cols:
            continue

        missing_pct = df[col].isna().mean()
        if missing_pct > threshold:
            result[col] = missing_pct

    return result
