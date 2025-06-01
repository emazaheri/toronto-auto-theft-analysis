"""ETL package for Toronto Auto Theft Analysis.

This package contains modules for extracting, transforming, and loading
Toronto auto theft data. It follows a modular approach with separate
components for each stage of the ETL process.

Modules:
    extractors: Data extraction from raw sources
    transformers: Data cleaning, validation, and feature engineering
    loaders: Data storage in processed formats
    auto_theft_pipeline: Main ETL pipeline orchestration for auto theft data
    census_pipeline: ETL pipeline orchestration for Canada Census 2021 data
    geospatial_pipeline: ETL pipeline for FSA and neighbourhood spatial data
"""
