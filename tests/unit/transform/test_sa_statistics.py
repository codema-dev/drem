from pathlib import Path
from typing import Dict

import geopandas as gpd
import pandas as pd
import pytest

from geopandas.testing import assert_geodataframe_equal
from icontract import ViolationError
from pandas.testing import assert_frame_equal
from shapely.geometry import Point
from shapely.geometry import Polygon

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.sa_statistics import _convert_columns_to_dict
from drem.transform.sa_statistics import _extract_column_names_via_glossary
from drem.transform.sa_statistics import _extract_rows_from_glossary
from drem.transform.sa_statistics import _get_columns
from drem.transform.sa_statistics import _link_small_areas_to_postcodes
from drem.transform.sa_statistics import _melt_columns
from drem.transform.sa_statistics import _merge_with_geometries
from drem.transform.sa_statistics import _pivot_table
from drem.transform.sa_statistics import _rename_columns_via_glossary
from drem.transform.sa_statistics import _replace_substring_in_column
from drem.transform.sa_statistics import _split_column_in_two_on_substring
from drem.transform.sa_statistics import _strip_column


STATS_IN: Path = UTEST_DATA_TRANSFORM / "sa_statistics_raw.csv"
GLOSSARY: Path = UTEST_DATA_TRANSFORM / "sa_glossary.xlsx"
SA_GEOMETRIES: Path = UTEST_DATA_TRANSFORM / "dublin_sa_geometries_clean.parquet"
POSTCODE_GEOMETRIES: Path = UTEST_DATA_TRANSFORM / "dublin_postcodes_clean.parquet"

EXTRACT_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_extract_year_built.csv"
MELT_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_melt_year_built.csv"
CLEANED_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_clean_year_built.csv"

STATS_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_statistics_clean.parquet"


@pytest.fixture
def raw_glossary() -> pd.DataFrame:
    """Create Raw Small Area Statistics Glossary.

    Returns:
        pd.DataFrame: Raw glossary table
    """
    return pd.DataFrame(
        {
            "Tables Within Themes": [
                "Table 1",
                "Private households by type of accommodation ",
                "Table 2",
                "Permanent private households by year built ",
                "Table 5",
                "Permanent private households by central heating ",
            ],
            "Column Names": [
                "T6_1_HB_H",
                "T6_1_FA_H",
                "T6_2_PRE19H",
                "T6_2_19_45H",
                "T6_5_NCH",
                "T6_5_OCH",
            ],
            "Description of Field": [
                "House/Bungalow (No. of households)",
                "Flat/Apartment (No. of households)",
                "Pre 1919 (No. of households)",
                "1919 - 1945 (No. of households)",
                "No central heating",
                "Oil",
            ],
        },
    )


@pytest.fixture
def raw_year_built_glossary() -> pd.DataFrame:
    """Create Raw Year Built Glossary.

    Returns:
        pd.DataFrame: Raw Year built glossary table
    """
    return pd.DataFrame(
        {
            "Tables Within Themes": [
                "Table 2",
                "Permanent private households by year built ",
            ],
            "Column Names": ["T6_2_PRE19H", "T6_2_19_45H"],
            "Description of Field": [
                "Pre 1919 (No. of households)",
                "1919 - 1945 (No. of households)",
            ],
        },
    )


@pytest.fixture
def year_built_glossary() -> Dict[str, str]:
    """Create Year built glossary.

    Returns:
        Dict[str, str]: Maps Year Built encodings to values
    """
    return {
        "T6_2_PRE19H": "Pre 1919 (No. of households)",
        "T6_2_19_45H": "1919 - 1945 (No. of households)",
    }


@pytest.fixture
def raw_statistics() -> pd.DataFrame:
    """Create Raw Small Area Statistics.

    Returns:
        pd.DataFrame: Raw Statistics
    """
    return pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "T6_1_HB_H": [2],
            "T6_1_FA_H": [3],
            "T6_2_PRE19H": [10],
            "T6_2_19_45H": [20],
            "T6_5_NCH": [7],
            "T6_5_OCH": 12,
        },
    )


def test_extracted_year_built_table_from_glossary_matches_expected(
    raw_glossary: pd.DataFrame, raw_year_built_glossary: pd.DataFrame,
) -> None:
    """Extracted table matches matches expected table.

    Args:
        raw_glossary (pd.DataFrame): Raw glossary table
        raw_year_built_glossary (pd.DataFrame): Raw Year built glossary table
    """
    expected_output = raw_year_built_glossary

    output = _extract_rows_from_glossary.run(
        raw_glossary,
        target="Tables Within Themes",
        table_name="Permanent private households by year built ",
    )

    assert_frame_equal(output, expected_output)


def test_convert_columns_to_dict_as_expected(
    raw_year_built_glossary: pd.DataFrame, year_built_glossary: pd.DataFrame,
) -> None:
    """Extract 2 DataFrame columns and convert to Dict.

    Args:
        raw_year_built_glossary (pd.DataFrame): Raw Year built glossary table
        year_built_glossary (pd.DataFrame): Year built glossary
    """
    expected_output = year_built_glossary

    output = _convert_columns_to_dict.run(
        raw_year_built_glossary,
        column_name_index="Column Names",
        column_name_values="Description of Field",
    )

    assert output == expected_output


def test_extract_year_built_column_names_via_glossary(
    raw_statistics: pd.DataFrame, year_built_glossary: Dict[str, str],
) -> None:
    """Extract year built column names from DataFrame via glossary.

    Args:
        raw_statistics (pd.DataFrame):  Raw Statistics
        year_built_glossary (Dict[str, str]): Year built glossary
    """
    expected_output = pd.DataFrame(
        {"GEOGID": ["SA2017_017001001"], "T6_2_PRE19H": [10], "T6_2_19_45H": [20]},
    )

    output = _extract_column_names_via_glossary.run(
        raw_statistics, year_built_glossary, additional_columns=["GEOGID"],
    )

    assert_frame_equal(output, expected_output)


def test_extract_column_names_via_glossary_raises_error_with_unknown_columns(
    raw_statistics: pd.DataFrame, year_built_glossary: Dict[str, str],
) -> None:
    """Raise error with unknown additional columns.

    Args:
        raw_statistics (pd.DataFrame):  Raw Statistics
        year_built_glossary (Dict[str, str]): Year built glossary
    """
    with pytest.raises(ViolationError):
        _extract_column_names_via_glossary.run(
            raw_statistics, year_built_glossary, additional_columns=["I break things"],
        )


def test_extract_column_names_via_glossary_raises_error_with_unknown_glossary(
    raw_statistics: pd.DataFrame,
) -> None:
    """Raise error with a dictionary with keys not in DataFrame columns.

    Args:
        raw_statistics (pd.DataFrame):  Raw Statistics
    """
    hot_potato_glossary = {"I break things": "in pandas"}

    with pytest.raises(ViolationError):
        _extract_column_names_via_glossary.run(
            raw_statistics, hot_potato_glossary, additional_columns=["GEOGID"],
        )


def test_rename_columns_via_glossary(year_built_glossary: Dict[str, str]) -> None:
    """Rename year built column names via glossary.

    Args:
        year_built_glossary (Dict[str, str]): Year built glossary
    """
    before_renaming = pd.DataFrame(
        {"GEOGID": ["SA2017_017001001"], "T6_2_PRE19H": [10], "T6_2_19_45H": [20]},
    )
    expected_output = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "Pre 1919 (No. of households)": [10],
            "1919 - 1945 (No. of households)": [20],
        },
    )

    output = _rename_columns_via_glossary.run(before_renaming, year_built_glossary)

    assert_frame_equal(output, expected_output)


def test_melt_columns() -> None:
    """Melt selected columns to rows."""
    before_melt = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "Pre 1919 (No. of households)": [10],
            "Pre 1919 (No. of persons)": [25],
        },
    )
    expected_output = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001", "SA2017_017001001"],
            "variable": ["Pre 1919 (No. of households)", "Pre 1919 (No. of persons)"],
            "value": [10, 25],
        },
    )

    output = _melt_columns.run(before_melt, id_vars="GEOGID")

    assert_frame_equal(output, expected_output)


def test_split_column_in_two_on_substring() -> None:
    """Split column in two on substring."""
    before_split = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "variable": ["Pre 1919 (No. of households)"],
            "value": [10],
        },
    )
    expected_output = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "variable": ["Pre 1919 (No. of households)"],
            "value": [10],
            "raw_year_built": ["Pre 1919 "],
            "raw_households_and_persons": ["No. of households)"],
        },
    )

    output = _split_column_in_two_on_substring.run(
        before_split,
        target="variable",
        pat=r"(",
        left_column_name="raw_year_built",
        right_column_name="raw_households_and_persons",
    )

    assert_frame_equal(output, expected_output)


def test_replace_substring_in_column() -> None:
    """Replace substring in column."""
    before_removal = pd.DataFrame({"dirty_column": ["No. of households)"]})
    expected_output = pd.DataFrame(
        {"dirty_column": ["No. of households)"], "clean_column": ["households"]},
    )

    output = _replace_substring_in_column.run(
        before_removal,
        target="dirty_column",
        result="clean_column",
        pat=r"(No. of )|(\))",
        repl="",
    )

    assert_frame_equal(output, expected_output)


def test_strip_column() -> None:
    """Strip column strings of whitespace."""
    before_strip = pd.DataFrame({"dirty_column": ["Pre 1919 "]})
    expected_output = pd.DataFrame(
        {"dirty_column": ["Pre 1919 "], "clean_column": ["Pre 1919"]},
    )

    output = _strip_column.run(
        before_strip, target="dirty_column", result="clean_column",
    )

    assert_frame_equal(output, expected_output)


def test_pivot_table() -> None:
    """Pivot table to expected format."""
    before_pivot_table = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001", "SA2017_017001001"],
            "households_and_persons": ["households", "persons"],
            "year_built": ["Pre 1919", "Pre 1919"],
            "value": [4, 20],
        },
    )
    expected_output = pd.DataFrame(
        {
            "GEOGID": ["SA2017_017001001"],
            "year_built": ["Pre 1919"],
            "households": [4],
            "persons": [20],
        },
    )

    output = _pivot_table.run(
        before_pivot_table,
        index=["GEOGID", "year_built"],
        values="value",
        columns="households_and_persons",
    )

    assert_frame_equal(output, expected_output, check_like=True)


def test_merge_with_geometries() -> None:
    """Geometries are added to Statistics."""
    statistics: pd.DataFrame = pd.DataFrame(
        {"small_area": [111], "very_important_data": [42]},
    )
    dublin_geometries: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {"small_area": [111], "geometry": [Point((0, 0))]},
    )
    expected_output: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {"small_area": [111], "geometry": [Point((0, 0))], "very_important_data": [42]},
    )

    output = _merge_with_geometries.run(statistics, dublin_geometries, on="small_area")

    assert_geodataframe_equal(output, expected_output)


def test_link_small_areas_to_postcodes() -> None:
    """Small Areas that are 'mostly' in Postcode are linked to Postcode."""
    small_areas = gpd.GeoDataFrame(
        {
            "period_built": ["before 1919", "after 2010"],
            "households": [3, 4],
            "people": [10, 12],
            "small_area": [1, 2],
            "geometry": [
                Polygon([(1, 0), (1, 1), (3, 1)]),
                Polygon([(1, 0), (1, 1), (0, 1)]),
            ],
        },
    )

    postcodes = gpd.GeoDataFrame(
        {
            "postcodes": ["Co. Dublin", "Dublin 1"],
            "geometry": [
                Polygon([(0, 0), (3, 0), (0, 3)]),  # only overlaps with small_area=1
                Polygon([(3, 3), (0, 3), (3, 0)]),  # mostly overlaps with small_area==1
            ],
        },
    )

    expected_output = gpd.GeoDataFrame(
        {
            "period_built": ["before 1919", "after 2010"],
            "households": [3, 4],
            "people": [10, 12],
            "small_area": [1, 2],
            "geometry": [
                Polygon([(1, 0), (1, 1), (3, 1)]),
                Polygon([(1, 0), (1, 1), (0, 1)]),
            ],
            "postcodes": ["Co. Dublin", "Co. Dublin"],
        },
    )

    output = _link_small_areas_to_postcodes.run(small_areas, postcodes)

    assert_geodataframe_equal(output, expected_output, check_like=True)


def test_get_columns_raises_error_if_passed_nonexistent_column_name() -> None:
    """Raise error if passed non-existent column name."""
    i_am_data = pd.DataFrame({"my_name_is": ["what"]})

    with pytest.raises(ViolationError):
        _get_columns.run(i_am_data, ["my_name_is", "i_dont_exist"])
