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
from tdda.referencetest.referencetest import ReferenceTest

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.sa_statistics import _clean_year_built_columns
from drem.transform.sa_statistics import _convert_columns_to_dict
from drem.transform.sa_statistics import _extract_column_names_via_glossary
from drem.transform.sa_statistics import _extract_dublin_small_areas
from drem.transform.sa_statistics import _extract_rows_from_glossary
from drem.transform.sa_statistics import _link_dublin_small_areas_to_geometries
from drem.transform.sa_statistics import _link_small_areas_to_postcodes
from drem.transform.sa_statistics import _melt_year_built_columns
from drem.transform.sa_statistics import _rename_columns_via_glossary


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

    output = _extract_rows_from_glossary(
        raw_glossary,
        target="Tables Within Themes",
        table_name="Permanent private households by year built ",
        number_of_rows=2,
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

    output = _convert_columns_to_dict(
        raw_year_built_glossary, columns=["Column Names", "Description of Field"],
    )

    assert output == expected_output


def test_convert_columns_to_dict_raises_error_with_invalid_arg() -> None:
    """Convert columns fails if > 2 columns passed."""
    input_table = pd.DataFrame(
        {"Column1": [0, 1, 2], "Column2": [0, 1, 2], "Column3": [0, 1, 2]},
    )
    with pytest.raises(ViolationError):
        _convert_columns_to_dict(
            input_table, columns=["Column1", "Column2", "Column3"],
        )


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

    output = _extract_column_names_via_glossary(
        raw_statistics, year_built_glossary, additional_columns=["GEOGID"],
    )

    assert_frame_equal(output, expected_output)


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

    output = _rename_columns_via_glossary(before_renaming, year_built_glossary)

    assert_frame_equal(output, expected_output)


def test_melt_year_built_columns(ref: ReferenceTest) -> None:
    """Melted columns match (tdda generated) reference file.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    statistics = pd.read_csv(EXTRACT_EOUT)

    output = _melt_year_built_columns(statistics)

    ref.assertDataFrameCorrect(output, MELT_EOUT)


def test_clean_year_built_columns(ref: ReferenceTest) -> None:
    """Cleaned columns match reference file.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    statistics = pd.read_csv(MELT_EOUT)

    output = _clean_year_built_columns(statistics)

    ref.assertDataFrameCorrect(output, CLEANED_EOUT)


def test_extract_dublin_small_areas() -> None:
    """Statistics are linked to Dublin data via small_area."""
    statistics: pd.DataFrame = pd.DataFrame({"small_area": [111, 222]})
    dublin_geometries: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {"small_area": [111], "geometry": [Point((0, 0))]},
    )
    expected_output: pd.DataFrame = pd.DataFrame({"small_area": [111]})

    output = _extract_dublin_small_areas(statistics, dublin_geometries)

    assert_frame_equal(output, expected_output)


def test_extract_dublin_small_areas_raises_error() -> None:
    """Function raises error if no common small_area column."""
    statistics = pd.DataFrame({"small_area": ["267088001", "077089001"]})
    geometries = gpd.GeoDataFrame({"SMALL_AREAS": ["267088001"]})

    with pytest.raises(ViolationError):
        _extract_dublin_small_areas(statistics, geometries)


def test_link_dublin_small_areas_to_geometries() -> None:
    """Geometries are added to Statistics."""
    statistics: pd.DataFrame = pd.DataFrame({"small_area": [111]})
    dublin_geometries: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {"small_area": [111], "geometry": [Point((0, 0))]},
    )
    expected_output: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {"small_area": [111], "geometry": [Point((0, 0))]},
    )

    output = _link_dublin_small_areas_to_geometries(statistics, dublin_geometries)

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

    output = _link_small_areas_to_postcodes(small_areas, postcodes)

    assert_geodataframe_equal(output, expected_output, check_like=True)


def test_link_small_areas_to_postcodes_raises_error() -> None:
    """Raises error if output columns do not precisely match contract."""
    small_areas = gpd.GeoDataFrame(
        {
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

    with pytest.raises(ViolationError):
        _link_small_areas_to_postcodes(small_areas, postcodes)
