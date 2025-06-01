"""Configuration settings for ETL process.

This module contains configuration settings for the ETL pipeline,
including file paths, data transformation parameters, and validation rules.
"""

from pathlib import Path

# Define base directories
ROOT_DIR = Path(__file__).parents[2].absolute()
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "00_raw"
PROCESSED_DATA_DIR = DATA_DIR / "01_processed"

# Configuration for joined data pipeline
JOINED_DATA_CONFIG = {
    # Will be set in the joined_data_pipeline.py to avoid circular imports
}

# Census data configuration
CENSUS_CONFIG = {
    "geo_input_path": RAW_DATA_DIR / "census_2021_geo.csv",
    "data_input_path": RAW_DATA_DIR / "census_2021.csv",
    "output_path": PROCESSED_DATA_DIR / "census_2021_processed.parquet",
    "column_dtypes": {
        "DGUID": "category",
        "ALT_GEO_CODE": "object",
        "CHARACTERISTIC_ID": "category",
        "CHARACTERISTIC_NAME": "object",
        "CHARACTERISTIC_NOTE": "Int16",
        "DATA_QUALITY_FLAG": "category",
        "TNR_SF": "float32",
        "TNR_LF": "float32",
    },
    "columns_to_drop": [
        "CENSUS_YEAR",
        "GEO_LEVEL",
        "GEO_NAME",
    ],
    "fsa_prefix": "M",  # Toronto FSAs start with "M"
    "encoding": "latin1",
    "max_characteristic_level": 4,  # Filter characteristics up to this level
}

# Auto theft data configuration
AUTO_THEFT_CONFIG = {
    "input_path": RAW_DATA_DIR / "auto_theft.csv",
    "output_path": PROCESSED_DATA_DIR / "auto_theft_processed.parquet",
    "date_columns": ["REPORT_DATE", "OCC_DATE"],
    "column_dtypes": {
        "EVENT_UNIQUE_ID": "object",
        "REPORT_YEAR": "Int16",
        "REPORT_MONTH": "category",
        "REPORT_DAY": "Int16",
        "REPORT_DOY": "Int16",
        "REPORT_HOUR": "Int16",
        "OCC_YEAR": "Int16",
        "OCC_MONTH": "category",
        "OCC_DAY": "Int16",
        "OCC_DOY": "Int16",
        "OCC_HOUR": "Int16",
        "DIVISION": "category",
        "LOCATION_TYPE": "category",
        "PREMISES_TYPE": "category",
        "HOOD_158": "category",
        "NEIGHBOURHOOD_158": "category",
        "LONG_WGS84": "float64",
        "LAT_WGS84": "float64",
    },
    "columns_to_drop": [
        "OBJECTID",
        "OFFENCE",
        "MCI_CATEGORY",
        "HOOD_140",
        "NEIGHBOURHOOD_140",
        "x",
        "y",
        "UCR_CODE",
        "UCR_EXT",
    ],
    "na_values": {
        "LAT_WGS84": [0, "0", "0.0"],
        "LONG_WGS84": [0, "0", "0.0"],
        "DIVISION": ["NSA"],
        "HOOD_158": ["NSA"],
        "NEIGHBOURHOOD_158": ["NSA"],
    },
    "converters": {"REPORT_DOW": str.strip, "OCC_DOW": str.strip},
    # Validation parameters
    "coord_validation": {
        "lat_min": 43.5,
        "lat_max": 44.0,
        "long_min": -79.8,
        "long_max": -79.0,
    },
    "valid_years": range(2013, 2025),  # 2014 to 2024 inclusive
    # Feature engineering parameters
    "hour_bins": [-1, 5, 11, 17, 21, 23],
    "hour_labels": ["Night", "Morning", "Afternoon", "Evening", "Night"],
    "season_map": {
        "January": "Winter",
        "February": "Winter",
        "March": "Spring",
        "April": "Spring",
        "May": "Spring",
        "June": "Summer",
        "July": "Summer",
        "August": "Summer",
        "September": "Autumn",
        "October": "Autumn",
        "November": "Autumn",
        "December": "Winter",
    },
    "season_order": ["Winter", "Spring", "Summer", "Autumn"],
    "weekend_days": ["Saturday", "Sunday"],
}
