"""Data extraction module for Toronto geospatial data (FSAs and Neighbourhoods)."""

import geopandas as gpd

from src.config.etl_config import GEOSPATIAL_CONFIG
from src.config.logging_config import get_logger
from src.utils.error_handling import retry

logger = get_logger(__name__)


class GeospatialExtractor:
    """Extractor class for Toronto geospatial data.

    This class is responsible for extracting GeoJSON data for Toronto's
    neighbourhoods and Forward Sortation Areas (FSAs) used in postal codes.
    """

    def __init__(self, config: dict | None = None):
        """Initialize the extractor with configuration.

        Args:
            config: Configuration dictionary overriding default settings
        """
        self.config = config or GEOSPATIAL_CONFIG
        self.hood_input_path = self.config["hood_input_path"]
        self.fsa_input_path = self.config["fsa_input_path"]
        self.crs = self.config["crs"]

        logger.info(
            f"GeospatialExtractor initialized with: "
            f"neighbourhood path: {self.hood_input_path}, "
            f"FSA path: {self.fsa_input_path}"
        )

    @retry(IOError, tries=3, delay=2.0)
    def extract_hood_data(self) -> gpd.GeoDataFrame:
        """Load the Toronto neighbourhoods GeoJSON file.

        Returns:
            GeoDataFrame with Toronto neighbourhood boundaries

        Raises:
            IOError: If there's an error reading the file
        """
        logger.info(f"Reading neighbourhoods from {self.hood_input_path}")

        try:
            hoods = gpd.read_file(self.hood_input_path)
            logger.info(f"Successfully loaded {len(hoods)} neighbourhoods")
            return hoods
        except Exception as e:
            logger.error(f"Failed to read neighbourhoods file: {e}")
            raise

    @retry(IOError, tries=3, delay=2.0)
    def extract_fsa_data(self) -> gpd.GeoDataFrame:
        """Load the Toronto FSA GeoJSON file.

        Returns:
            GeoDataFrame with Toronto FSA boundaries

        Raises:
            IOError: If there's an error reading the file
        """
        logger.info(f"Reading FSAs from {self.fsa_input_path}")

        try:
            fsas = gpd.read_file(self.fsa_input_path)
            logger.info(f"Successfully loaded {len(fsas)} FSAs")
            return fsas
        except Exception as e:
            logger.error(f"Failed to read FSA file: {e}")
            raise

    def extract_data(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Extract both neighbourhoods and FSA data.

        Returns:
            Tuple containing (neighbourhoods GeoDataFrame, FSAs GeoDataFrame)
        """
        hoods = self.extract_hood_data()
        fsas = self.extract_fsa_data()
        return hoods, fsas
