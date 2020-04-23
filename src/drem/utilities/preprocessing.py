from typing import List, Union

import geopandas as gpd
import numpy as np
import pandas as pd
from icontract import require, ensure
from pandas.api.types import is_numeric_dtype, is_string_dtype
from prefect import task
from toolz import pipe

from codema_drem.utilities.icontract import no_null_values_in_result_dataframe_columns


# TODO: rewrite funcs to handle multiindex dfs


def infer_column_data_types(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Infers data types using pd.convert_dtypes - ignoring 'geometry' column 
    used in GeoDataFrames as pd.convert_dtypes interprets this column 
    incorrectly as a string.

    Parameters
    ----------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
    """

    names_of_non_geometry_columns = [
        column_name for column_name in df.columns if column_name != "geometry"
    ]

    df.loc[:, names_of_non_geometry_columns] = df[
        names_of_non_geometry_columns
    ].convert_dtypes()
    return df


@task
def convert_string_columns_to_categorical(
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Converts string cols to categorical to save memory.

    A string col stores every cell whereas a categorical col stores
    only the first instance of a cell (i.e. it splits the strings into distinct
    categories...)

    Parameters
    ----------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
        df with string cols converted to categorical
    """

    string_columns = df.select_dtypes(include=["object"])
    if not string_columns.empty:
        df.loc[:, string_columns.columns] = string_columns.astype("category")

    return df


@task
def downcast_float_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns df with float columns compressed to optimal size

    Source: https://medium.com/bigdatarepublic/advanced-pandas-optimize-speed-and-memory-a654b53be6c2

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    float_columns = df.select_dtypes(include=["float64"])
    if not float_columns.empty:
        df.loc[:, float_columns.columns] = float_columns.apply(
            pd.to_numeric, downcast="float"
        )

    return df


@task
def downcast_int_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns df with int columns compressed to optimal size

    Source: https://medium.com/bigdatarepublic/advanced-pandas-optimize-speed-and-memory-a654b53be6c2

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    int_columns = df.select_dtypes(include=["int64"])
    if not int_columns.empty:
        df.loc[:, int_columns.columns] = int_columns.apply(
            pd.to_numeric, downcast="integer"
        )

    return df


@task
def convert_mostly_empty_float_columns_to_sparse(df: pd.DataFrame,) -> pd.DataFrame:
    """Convert float columns with mostly empty rows to
    sparse to save memory

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    float_columns = df.select_dtypes(include=["float64", "float32", "float16"])
    if not float_columns.empty:
        for column in float_columns:
            if df[column].isna().sum() > df[column].notna().sum():
                df.loc[:, column] = df[column].astype(f"Sparse[float]")

    return df


@task
def convert_mostly_zero_float_columns_to_sparse(df: pd.DataFrame,) -> pd.DataFrame:
    """Convert float columns with mostly empty rows to
    sparse to save memory

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    float_columns = df.select_dtypes(include=["float64", "float32", "float16"])
    if not float_columns.empty:
        for column in float_columns:
            zero_rows = df[column] == 0.0
            if df.loc[zero_rows, column].count() > df.loc[~zero_rows, column].count():
                df.loc[:, column] = df[column].astype(
                    pd.SparseDtype(np.float64, fill_value=0)
                )

    return df


@task
def convert_mostly_zero_int_columns_to_sparse(df: pd.DataFrame,) -> pd.DataFrame:
    """Convert int columns with mostly empty rows to sparse to save memory

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    int_columns = df.select_dtypes(include=["int64", "int32", "int16", "int8"])
    if not int_columns.empty:
        for column in int_columns:
            zero_rows = df[column] == 0
            if df.loc[zero_rows, column].count() > df.loc[~zero_rows, column].count():
                df.loc[:, column] = df[column].astype(
                    pd.SparseDtype(np.int64, fill_value=0)
                )

    return df


@task
# @ensure(
#     lambda result: no_null_values_in_result_dataframe_columns(
#         result.select_dtypes(include=["float64", "float32", "float16"]).columns, result
#     )
# )
def replace_blanks_with_zero_in_float_columns(
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Replace empty values in float columns with 0

    Parameters
    ----------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
        df with null values replaced
    """

    float_columns = df.select_dtypes(include=["float64", "float32", "float16"])

    if not float_columns.empty:
        df.loc[:, float_columns.columns] = float_columns.fillna(0)

    return df


@task
def set_string_columns_to_lowercase(
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Sets all strings in column to lowercase

    Warning! (1) Converts categorical columns back to string...
    (2) Converts numeric to NaN...

    Parameters
    ----------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
    """

    string_columns = df.select_dtypes(include=["category", "object"])
    df.loc[:, string_columns.columns] = string_columns.apply(
        lambda col: col.str.lower(), axis=1
    )

    return df
