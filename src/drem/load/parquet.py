#!/usr/bin/env python

from pathlib import Path
from typing import Union

from geopandas import GeoDataFrame
from pandas import DataFrame
from prefect import Task


class LoadToParquet(Task):
    """A generic Task class to load Data to parquet files.

    Args:
        Task (Task): A Prefect Task class

    For more information see:
        https://docs.prefect.io/core/concepts/tasks.html#overview
    """

    def run(self, df: Union[DataFrame, GeoDataFrame], filepath: Path) -> None:
        """Load pandas DataFrame or geopandas GeoDataFrame to local parquet file.

        Args:
            df (Union[DataFrame, GeoDataFrame]): In-memory data to be saved to disk
            filepath (Path): Save path for data
        """
        df.to_parquet(filepath)
