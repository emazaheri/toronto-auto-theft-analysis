"""Utility functions for error handling in ETL processes."""

import functools
import time
from collections.abc import Callable
from typing import (
    Any,
    TypeVar,
    cast,
)

from src.config.logging_config import get_logger

logger = get_logger(__name__)

# Type variables for generic function annotations
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


def retry(
    exceptions: type[Exception] | tuple[type[Exception], ...],
    tries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
) -> Callable[[F], F]:
    """Retry decorator with exponential backoff.

    Args:
        exceptions: The exception(s) to catch and retry on
        tries: Number of times to try before giving up
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier (e.g. value of 2 will double the delay each retry)

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            mtries, mdelay = tries, delay

            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(
                        f"Exception {e} occurred, retrying in {mdelay} seconds... "
                        f"({mtries-1} attempts remaining)"
                    )
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff

            # Last try
            return func(*args, **kwargs)

        return cast("F", wrapper)

    return decorator


def handle_missing_file(default_value: T) -> Callable[[F], Callable[..., T]]:
    """Decorator to handle missing file exceptions by returning a default value.

    Args:
        default_value: The value to return if the file is missing

    Returns:
        Decorated function that returns default_value on FileNotFoundError
    """

    def decorator(func: F) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                logger.error(f"File not found: {e}")
                return default_value

        return wrapper

    return decorator
