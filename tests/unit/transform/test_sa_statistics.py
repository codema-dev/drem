from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from geopandas.testing import assert_geodataframe_equal
from icontract import ViolationError
from shapely.geometry import Polygon
from tdda.referencetest.referencetest import ReferenceTest

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.sa_statistics import _clean_year_built_columns
from drem.transform.sa_statistics import _extract_dublin_small_areas
from drem.transform.sa_statistics import _extract_year_built
from drem.transform.sa_statistics import _link_small_areas_to_postcodes
from drem.transform.sa_statistics import _melt_year_built_columns


STATS_IN: Path = UTEST_DATA_TRANSFORM / "sa_statistics_raw.csv"
GLOSSARY: Path = UTEST_DATA_TRANSFORM / "sa_glossary.xlsx"
SA_GEOMETRIES: Path = UTEST_DATA_TRANSFORM / "dublin_sa_geometries_clean.parquet"
POSTCODE_GEOMETRIES: Path = UTEST_DATA_TRANSFORM / "dublin_postcodes_clean.parquet"

EXTRACT_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_extract_year_built.csv"
MELT_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_melt_year_built.csv"
CLEANED_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_clean_year_built.csv"

STATS_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_statistics_clean.parquet"


def test_extract_year_built(ref: ReferenceTest) -> None:
    """Extracted columns match reference file.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    statistics = pd.read_csv(STATS_IN)
    glossary = pd.read_excel(GLOSSARY, engine="openpyxl")

    output = _extract_year_built(statistics, glossary)

    ref.assertDataFrameCorrect(output, EXTRACT_EOUT)


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


def test_extract_dublin_small_areas_raises_error() -> None:
    """Function raises error if no common small_area column."""
    statistics = pd.DataFrame({"small_area": ["267088001", "077089001"]})
    geometries = gpd.GeoDataFrame({"SMALL_AREAS": ["267088001"]})

    with pytest.raises(ViolationError):
        _extract_dublin_small_areas(statistics, geometries)


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
