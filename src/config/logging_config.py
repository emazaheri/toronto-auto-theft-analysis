"""Logging configuration for the ETL pipeline.

This module contains the logging configuration for the ETL pipeline,
including log format, handlers, and level settings.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Get the root directory
ROOT_DIR = Path(__file__).parents[2].absolute()
LOG_DIR = ROOT_DIR / "logs"

# Create logs directory if it doesn't exist
LOG_DIR.mkdir(exist_ok=True)

# Log file paths
ETL_LOG_FILE = LOG_DIR / "etl_pipeline.log"

# Define the logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
        "simple": {"format": "%(levelname)s - %(message)s"},
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": sys.stdout,
        },
        "file": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "verbose",
            "filename": str(ETL_LOG_FILE),
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
        },
    },
    "loggers": {
        "etl": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        }
    },
}


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger.

    Args:
        name: The name of the logger.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)

    # Configure logger if it doesn't have handlers yet
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
        # Make sure log directory exists
        LOG_DIR.mkdir(exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(ETL_LOG_FILE),
            maxBytes=10485760,  # 10 MB
            backupCount=5,
        )

        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        file_handler.setLevel(logging.DEBUG)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

    return logger
