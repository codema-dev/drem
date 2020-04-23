import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
import pytest

from io import StringIO
from pandas.api.types import is_string_dtype, is_numeric_dtype
from pandas.testing import assert_frame_equal
from codema_drem.utilities.preprocessing import (
    infer_column_data_types,
    convert_string_columns_to_categorical,
    downcast_float_columns,
    downcast_int_columns,
    convert_mostly_empty_float_columns_to_sparse,
    convert_mostly_zero_float_columns_to_sparse,
    convert_mostly_zero_int_columns_to_sparse,
    replace_blanks_with_zero_in_float_columns,
    set_string_columns_to_lowercase,
)


@pytest.fixture()
def gdf() -> gpd.GeoDataFrame:

    return gpd.GeoDataFrame(
        {
            "string_col": pd.Series(["dublin 12", "dublin 19"]),
            "int_col": pd.Series([1, 2]),
            "float_col": pd.Series([1.1, 2.2]),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)]),
        }
    )


def test_convert_string_columns_to_categorical(gdf) -> None:

    input = gdf

    expected_output = gpd.GeoDataFrame(
        {
            "string_col": pd.Series(["dublin 12", "dublin 19"], dtype="category"),
            "int_col": pd.Series([1, 2]),
            "float_col": pd.Series([1.1, 2.2]),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)]),
        }
    )

    output = convert_string_columns_to_categorical.run(input)

    assert_frame_equal(output, expected_output)


def test_downcast_float_columns(gdf) -> None:

    input = gdf

    expected_output = gpd.GeoDataFrame(
        {
            "string_col": pd.Series(["dublin 12", "dublin 19"]),
            "int_col": pd.Series([1, 2]),
            "float_col": pd.Series([1.1, 2.2], dtype=np.float32),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)]),
        }
    )

    output = downcast_float_columns.run(input)

    assert_frame_equal(output, expected_output)


def test_downcast_int_columns(gdf) -> None:

    input = gdf

    expected_output = gpd.GeoDataFrame(
        {
            "string_col": pd.Series(["dublin 12", "dublin 19"]),
            "int_col": pd.Series([1, 2], dtype=np.int8),
            "float_col": pd.Series([1.1, 2.2]),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)]),
        }
    )

    output = downcast_int_columns.run(input)

    assert_frame_equal(output, expected_output)


def test_convert_mostly_empty_float_columns_to_sparse() -> None:

    input = pd.DataFrame(
        {
            "mostly_full_float_col": pd.Series([1.1, np.nan, 1.1, 1.1]),
            "mostly_empty_float_col": pd.Series([1.1, np.nan, np.nan, np.nan]),
        }
    )
    # NOTE: can't create an integer column with null values!

    expected_output = pd.DataFrame(
        {
            "mostly_full_float_col": pd.Series([1.1, np.nan, 1.1, 1.1]),
            "mostly_empty_float_col": pd.Series(
                [1.1, np.nan, np.nan, np.nan], dtype="Sparse[float64, nan]"
            ),
        }
    )

    output = convert_mostly_empty_float_columns_to_sparse.run(input)

    assert_frame_equal(output, expected_output)


def test_convert_mostly_zero_float_columns_to_sparse() -> None:

    input = pd.DataFrame(
        {
            "nozeros_float_col": pd.Series([1.1, 1.1, 1.1, 1.1]),
            "mostly_nonzero_float_col": pd.Series([1.1, 0, 1.1, 1.1]),
            "mostly_zero_float_col": pd.Series([1.1, 0, 0, 0]),
        }
    )
    # NOTE: can't create an integer column with null values!

    expected_output = pd.DataFrame(
        {
            "nozeros_float_col": pd.Series([1.1, 1.1, 1.1, 1.1]),
            "mostly_nonzero_float_col": pd.Series([1.1, 0, 1.1, 1.1]),
            "mostly_zero_float_col": pd.Series(
                [1.1, 0, 0, 0], dtype=pd.SparseDtype(np.float64, fill_value=0),
            ),
        }
    )

    output = convert_mostly_zero_float_columns_to_sparse.run(input)

    assert_frame_equal(output, expected_output)


@pytest.mark.xfail
def test_convert_mostly_zero_int_columns_to_sparse() -> None:

    input = pd.DataFrame(
        {
            "nozeros_int_col": pd.Series([1, 1, 1, 1]),
            "mostly_nonzero_int_col": pd.Series([1, 0, 1, 1]),
            "mostly_zero_int_col": pd.Series([1, 0, 0, 0]),
        }
    )
    # NOTE: can't create an integer column with null values!

    expected_output = pd.DataFrame(
        {
            "nozeros_int_col": pd.Series([1, 1, 1, 1]),
            "mostly_nonzero_int_col": pd.Series([1, 0, 1, 1]),
            "mostly_zero_int_col": pd.Series(
                [1, 0, 0, 0], dtype=pd.SparseDtype(np.int64, fill_value=0)
            ),
        }
    )
    # pd.SparseDtype(np.int64, fill_value=0)

    output = convert_mostly_zero_int_columns_to_sparse.run(input)

    assert_frame_equal(output, expected_output)


def test_set_string_columns_to_lowercase() -> None:

    input = gpd.GeoDataFrame(
        {
            "string_column": pd.Series(["BLAH", "BLAH"],),
            "categorical_column": pd.Series(["BLAH", "BLAH"], dtype="category"),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)],),
        }
    )

    expected_output = gpd.GeoDataFrame(
        {
            "string_column": pd.Series(["blah", "blah"],),
            "categorical_column": pd.Series(["blah", "blah"]),
            "geometry": gpd.GeoSeries([Point(0, 0), Point(0, 0)]),
        }
    )

    output = set_string_columns_to_lowercase.run(input)

    assert_frame_equal(output, expected_output)


def test_replace_blanks_with_zero_in_float_columns() -> None:

    input = pd.DataFrame(
        {
            "float64_column": pd.Series([1.1, 2.2, np.nan], dtype=np.float64),
            "float32_column": pd.Series([1.1, 2.2, np.nan], dtype=np.float32),
            "float16_column": pd.Series([1.1, 2.2, np.nan], dtype=np.float16),
        }
    )

    expected_output = pd.DataFrame(
        {
            "float64_column": pd.Series([1.1, 2.2, 0], dtype=np.float64),
            "float32_column": pd.Series([1.1, 2.2, 0], dtype=np.float32),
            "float16_column": pd.Series([1.1, 2.2, 0], dtype=np.float16),
        }
    )

    output = replace_blanks_with_zero_in_float_columns.run(input)

    assert_frame_equal(output, expected_output)
