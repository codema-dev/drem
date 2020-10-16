from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd

from prefect import task


@task
def read_parquet(filepath: Path, **kwargs: Any) -> dd.DataFrame:
    """Read a Parquet file into a Dask DataFrame.

    See https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame

    Args:
        filepath (Path): Path to Dask-compatible Parquet file
        **kwargs (Any): Passed to dask.dataframe.to_parquet

    Returns:
        dd.DataFrame: Dask DataFrame
    """
    return dd.read_parquet(filepath, **kwargs)


@task
def compute(ddf: dd.DataFrame) -> pd.DataFrame:
    """Compute this dask collection.

    This turns a lazy Dask collection into its in-memory equivalent.  A Dask dataframe
        turns into a Pandas dataframe. See
        https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame

    Args:
        ddf (dd.DataFrame): Dask DataFrame

    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    return ddf.compute()
