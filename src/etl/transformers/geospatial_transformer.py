"""Data transformation module for Toronto geospatial data (FSAs and Neighbourhoods)."""

import geopandas as gpd

from src.config.etl_config import GEOSPATIAL_CONFIG
from src.config.logging_config import get_logger

logger = get_logger(__name__)


class GeospatialTransformer:
    """Transformer class for Toronto geospatial data.

    This class is responsible for calculating the spatial relationship between
    Toronto's neighbourhoods and Forward Sortation Areas (FSAs) using
    areal-weighted interpolation.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the transformer with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or GEOSPATIAL_CONFIG
        self.crs = self.config["crs"]
        self.min_overlap_percent = self.config["min_overlap_percent"]

        logger.info(
            f"GeospatialTransformer initialized with CRS: {self.crs}, "
            f"min overlap threshold: {self.min_overlap_percent}"
        )

    def transform_data(
        self, data_tuple: tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        """Transform geospatial data by calculating spatial relationships.

        Args:
            data_tuple: Tuple containing (neighbourhoods GeoDataFrame,
                FSAs GeoDataFrame)

        Returns:
            GeoDataFrame with spatial intersection data and overlap percentages
        """
        hoods, fsas = data_tuple

        logger.info(
            f"Starting spatial transformation with {len(hoods)} neighbourhoods "
            f"and {len(fsas)} FSAs"
        )

        # Reproject to equal-area CRS for accurate area calculation
        hoods = hoods.to_crs(self.crs)
        fsas = fsas.to_crs(self.crs)
        logger.info(f"Reprojected data to {self.crs}")

        # Calculate FSA areas
        fsas["fsa_area"] = fsas.geometry.area

        # Perform spatial intersection
        logger.info("Calculating spatial intersection between FSAs and neighbourhoods")
        intersect = gpd.overlay(fsas, hoods, how="intersection")

        # Calculate intersection areas and overlap percentages
        intersect["intersect_area"] = intersect.geometry.area
        intersect = intersect.merge(fsas[["CFSAUID"]], on="CFSAUID")
        intersect["overlap_percent"] = (
            intersect["intersect_area"] / intersect["fsa_area"]
        )

        # Filter out negligible overlaps
        intersect = intersect[intersect["overlap_percent"] >= self.min_overlap_percent]
        logger.info(f"Found {len(intersect)} significant spatial intersections")

        # Select only the needed columns
        result = intersect[["AREA_LONG_CODE", "CFSAUID", "overlap_percent"]]

        logger.info("Spatial transformation completed successfully")
        return result
