from typing import Iterable

import pandas as pd

from icontract import require
from prefect import task


@task
@require(
    lambda df, column_names: set(column_names).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
def get_columns(df: pd.DataFrame, column_names: Iterable[str]) -> pd.DataFrame:
    """Get DataFrame columns (copy to a new DataFrame).

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        column_names (Iterable[str]): Names of columns to be extracted

    Returns:
        pd.DataFrame: A new DataFrame containing only the specified columns
    """
    return df.copy().loc[:, column_names]
