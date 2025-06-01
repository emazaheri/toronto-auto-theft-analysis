# Toronto Auto Theft Analysis

A data analytics project examining auto theft patterns in Toronto and their relationship to demographic factors using 2021 Census data.

## Project Overview

This project analyzes auto theft incidents in Toronto, Canada, combining police-reported theft data with demographic information from the 2021 Canadian Census. The analysis aims to identify patterns, trends, and potential socioeconomic factors associated with auto theft across Toronto's 158 official neighborhoods.

## Data Sources

- **Auto Theft Data**: Toronto Police Service auto theft incident reports
- **Census Data**: Statistics Canada 2021 Census, focusing on Toronto Forward Sortation Areas (FSAs)
- **Geospatial Data**:
  - Toronto's 158 official neighborhoods
  - Forward Sortation Areas (FSAs) boundaries

## Project Structure

```
toronto-auto-theft-analysis/
├── data/
│   ├── 00_raw/             # Raw data files
│   └── 01_processed/       # Processed data files
├── notebooks/              # Jupyter notebooks for analysis
├── src/                    # Source code for ETL and analysis
│   ├── config/             # Configuration settings
│   ├── etl/                # Extract, transform, load pipelines
│   │   ├── extractors/     # Data extraction modules
│   │   ├── transformers/   # Data transformation modules
│   │   └── loaders/        # Data loading modules
│   └── utils/              # Utility functions
├── logs/                   # Pipeline execution logs and metrics
├── tests/                  # Unit tests
└── report/                 # Analysis reports and visualizations
```

## ETL Pipelines

The project includes three ETL pipelines:

1. **Auto Theft Pipeline**: Processes Toronto Police Service auto theft data
   - Extracts raw incident data
   - Cleans and transforms the data
   - Outputs processed data to parquet format

2. **Census Pipeline**: Processes 2021 Canadian Census data
   - Extracts demographic data for Toronto FSAs
   - Filters and transforms the data
   - Outputs processed data to parquet format

3. **Geospatial Pipeline**: Processes spatial relationships between FSAs and neighborhoods
   - Calculates spatial intersections between FSAs and Toronto's 158 neighborhoods
   - Computes overlap percentages for area-weighted interpolation
   - Outputs FSA-to-neighborhood mapping data to parquet format

## Setup and Installation

### Prerequisites
- Python 3.10+
- Required packages listed in pyproject.toml

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/toronto-auto-theft-analysis.git
cd toronto-auto-theft-analysis
```

2. Install dependencies
```bash
pip install -e .
```

## Running the ETL Pipelines

### Run All Pipelines
```bash
python src/run_all_etl_pipelines.py
```

This will run all three pipelines in sequence. You can use the following options:
- `--auto-theft-only`: Run only the auto theft pipeline
- `--census-only`: Run only the census pipeline
- `--geospatial-only`: Run only the geospatial pipeline
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

### Individual Pipelines

You can also run each pipeline individually:

#### Auto Theft Data Pipeline
```bash
python src/run_etl_pipeline.py
```

#### Census Data Pipeline
```bash
python src/run_census_etl_pipeline.py
```

#### Geospatial Data Pipeline
```bash
python src/run_geospatial_etl_pipeline.py
```

## Analysis Notebooks

The analysis is documented in a series of Jupyter notebooks:

1. `01_eda_toronto_theft.ipynb` - Exploratory analysis of auto theft data
2. `02_eda_census.ipynb` - Exploratory analysis of census data
3. `03_fsa_to_neighborhood.ipynb` - Mapping FSAs to Toronto neighborhoods
4. `04_joint_theft_census_analysis.ipynb` - Combined analysis of theft and demographic factors

## License

[Add your license information here]

## Contributors

[Add contributor information here]
