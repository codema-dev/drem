from pathlib import Path
from typing import Any
from typing import Iterable
from typing import Union

import pandas as pd

from icontract import require
from prefect import task


@task
@require(
    lambda df, column_names: set(column_names).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
@require(lambda df: isinstance(df, pd.DataFrame))
def get_columns(df: pd.DataFrame, column_names: Iterable[str]) -> pd.DataFrame:
    """Get DataFrame columns (copy to a new DataFrame).

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        column_names (Iterable[str]): Names of columns to be extracted

    Returns:
        pd.DataFrame: A new DataFrame containing only the specified columns
    """
    return df.copy().loc[:, column_names]


@task
@require(
    lambda df, target: set(target).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
@require(lambda df: isinstance(df, pd.DataFrame))
def get_sum_of_columns(
    df: pd.DataFrame, target: Union[str, Iterable[str]], result: str,
) -> pd.DataFrame:
    """Get sum of target DataFrame columns.

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        target (Union[str, Iterable[str]]): Names of columns to be summed
        result (str): Name of result column

    Returns:
        pd.DataFrame: [description]
    """
    df[result] = df[target].copy().sum(axis=1)

    return df


@task
@require(lambda df: isinstance(df, pd.DataFrame))
def get_rows_where_column_contains_substring(
    df: pd.DataFrame, target: str, substring: str,
) -> pd.DataFrame:
    """Get rows where target columns contains substring.

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        target (str): Name of target column
        substring (str): Substring to be queried in target column

    Returns:
        pd.DataFrame: A copy of df containing only rows where column contains substring
    """
    rows = df[target].str.contains(substring)
    return df.copy()[rows].reset_index(drop=True)


@task
def rename(df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Alter axes labels.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html

    Args:
        df (pd.DataFrame): Any DataFrame
        **kwargs (Any): Keyword arguments to pass to pandas

    Returns:
        pd.DataFrame: DataFrame with axes labels altered
    """
    return df.rename(**kwargs)


@task
def read_parquet(dirpath: Path, filename: str) -> pd.DataFrame:
    """Read parquet.

    Args:
        dirpath (Path): Path to directory containing file
        filename (str): File name

    Returns:
        pd.DataFrame: DataFrame of parquet file
    """
    filepath = dirpath / f"{filename}.parquet"
    return pd.read_parquet(filepath)
