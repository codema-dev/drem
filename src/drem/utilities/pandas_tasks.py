from pathlib import Path
from typing import Any
from typing import Iterable
from typing import List
from typing import Union

import pandas as pd

from icontract import require
from prefect import task


@task
@require(
    lambda df, column_names: set(column_names).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
def get_columns(df: pd.DataFrame, column_names: Iterable[str]) -> pd.DataFrame:
    """Access a group of rows by label(s) or a boolean array.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.loc.html

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
def read_parquet(filepath: Path, **kwargs: Any) -> pd.DataFrame:
    """Load a parquet object from the file path, returning a DataFrame.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html

    Args:
        filepath (Path): Path to file
        **kwargs (Any): Passed to pandas.read_parquet

    Returns:
        pd.DataFrame: DataFrame
    """
    return pd.read_parquet(filepath, **kwargs)


@task
def read_html(filepath: Path, **kwargs: Any) -> List[pd.DataFrame]:
    """Read HTML tables into a list of DataFrame objects.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_html.html

    Args:
        filepath (Path): Path to file
        **kwargs (Any): Passed to pandas.read_html

    Returns:
        List[pd.DataFrame]: A list of DataFrames.
    """
    return pd.read_html(str(filepath), **kwargs)


@task
def replace_substring_in_column(
    df: pd.DataFrame, target: str, result: str, **kwargs: Any,
) -> pd.DataFrame:
    """Replace substring in DataFrame string column.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.replace.html

    Args:
        df (pd.DataFrame): DataFrame
        target (str): Name of target column
        result (str): Name of result column
        **kwargs (Any): Passed to pandas.Series.str.replace

    Returns:
        pd.DataFrame: A copy of the object with all matching occurrences of pat replaced by repl.
    """
    df = df.copy()

    df[result] = df[target].astype(str).str.replace(**kwargs)

    return df


@task
def dropna(df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Remove missing values.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.dropna.html

    Args:
        df (pd.DataFrame): DataFrame
        **kwargs (Any): Passed to pandas.DataFrame.dropna

    Returns:
        pd.DataFrame: A copy of the DataFrame with NA entries dropped from it.
    """
    df = df.copy()

    return df.dropna(**kwargs)


@task
def merge(left: pd.DataFrame, right: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Merge DataFrame or named Series objects with a database-style join.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html

    Args:
        left (pd.DataFrame): Object to be merged.
        right (pd.DataFrame): Object to merge with.
        **kwargs (Any): Passed to pandas.DataFrame.merge

    Returns:
        pd.DataFrame: A DataFrame of the two merged objects.
    """
    left = left.copy()

    return left.merge(right, **kwargs)


@task
def get_rows_by_index(df: pd.DataFrame, row_indexes: Iterable[str]) -> pd.DataFrame:
    """Access a group of rows by integer-location based indexing.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.loc.html

    Args:
        df (pd.DataFrame): DataFrame
        row_indexes (Iterable[str]): Names of rows to be extracted

    Returns:
        pd.DataFrame: DataFrame
    """
    return df.copy().iloc[row_indexes, :]


@task
def concat(**kwargs: Any) -> pd.DataFrame:
    """Concatenate pandas objects along a particular axis with optional set logic along the other axes.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html

    Args:
        **kwargs (Any): Passed to pandas.concat

    Returns:
        pd.DataFrame: DataFrame
    """
    return pd.concat(**kwargs)


@task
def replace(
    df: pd.DataFrame, target: str, result: str, to_replace: Any, value: Any,
) -> pd.DataFrame:
    """Replace values given in to_replace with value.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html

    Args:
        df (pd.DataFrame): DataFrame
        target (str): Name of target column
        result (str): Name of result column
        to_replace (Any): Values that will be replaced
        value (Any): Value to replace any values matching to_replace with

    Returns:
        pd.DataFrame: DataFrame
    """
    df = df.copy()

    df[result] = df[target].replace(to_replace, value)

    return df


@task
def groupby_sum(df: pd.DataFrame, by: Iterable[str], target: str) -> pd.DataFrame:
    """Group DataFrame using a mapper or by a Series of columns.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html

    Args:
        df (pd.DataFrame): DataFrame
        by (Iterable[str]): The names of columns by to be grouped.
        target (str): The name of the column to be summed.

    Returns:
        pd.DataFrame: DataFrame
    """
    df = df.copy()

    return df.groupby(by=by, as_index=False)[target].sum()
