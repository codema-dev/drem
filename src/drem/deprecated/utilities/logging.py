import logging
from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd
from functools import wraps
from icontract import ensure

from typing import Union
from codema_drem.utilities.paths import LOG_DIR


def _add_stream_handler(logger: logging.Logger):

    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(stream_handler)

    return logger


def _add_file_handler(logger: logging.Logger, log_path: Path):

    file_handler = logging.FileHandler(log_path, mode='w')
    formatter = logging.Formatter(
        fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')

    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    logger.addHandler(file_handler)

    return logger


def create_logger(caller: str) -> logging.Logger:

    log_path = LOG_DIR / f'{caller}.log'
    logger = logging.getLogger(caller)
    root = logging.getLogger()

    logger.setLevel(logging.DEBUG)

    """ Restart handlers every time called ...
        (alternatively could do if not logger.handlers
        and only initialise if haven't already been called)"""
    if logger.handlers:
        logger.handlers = []

    _add_file_handler(logger=logger, log_path=log_path)
    _add_stream_handler(logger=logger)

    logger.info('Logging started.')

    # Delete the Qtconsole stderr handler
    # ... as it automatically logs both DEBUG & INFO to stderr
    if root.handlers:
        root.handlers = []

    return logger


@ensure(lambda logger: logger is not None)
def log_df(
    logger: logging.Logger = None,
    columns: int = 5,
    rows: int = 5,
) -> None:
    """ Logs the function's dataframe head, shape and dtypes
        to the log file associated with logger

        Arguments:
            logger {logging.LOGGER}
        Keyword Arguments:
            columns {int} -- sets the number of columns logged
            rows {int} -- sets the number of rows logged

        Returns:
            None
        """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            pd.set_option("display.max_rows", rows)
            pd.set_option("display.max_columns", columns)

            result = func(*args, **kwargs)
            logger.debug(f'{func.__name__}, output shape: {result.shape}')
            logger.debug(
                f'{func.__name__}, output dtypes: \n{result.dtypes}\n'
            )
            logger.debug(f'{func.__name__}: \n{result.head()}\n')

            return result
        return wrapper
    return decorator


def log_dataframe(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    logger: logging.Logger,
    name: str = 'DataFrame',
    max_rows: int = 5,
    max_columns: int = 10,
) -> None:

    pd.options.display.max_columns = max_columns
    pd.options.display.max_rows = max_rows

    logger.debug(f'{name}, head:\n {df.head()}\n----------\n')


def log_series(
    series: Union[pd.Series, gpd.GeoSeries],
    logger: logging.Logger,
    name: str = 'DataFrame',
    max_rows: int = 5,
    max_columns: int = 10,
) -> None:

    pd.options.display.max_columns = max_columns
    pd.options.display.max_rows = max_rows

    logger.debug(f'{name} head:\n {series.head()}\n----------\n')


def log_dataframes(*args, logger: logging.Logger) -> None:

    for gdf in args:
        logger.debug(f'DataFrame head:\n {gdf.head()}\n----------\n')
