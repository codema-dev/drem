from pathlib import Path

import geopandas as gpd
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Polygon
from tdda.referencetest.referencetest import ReferenceTest

from drem import transform_sa_statistics
from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform import sa_statistics


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

    output = sa_statistics._extract_year_built(statistics, glossary)

    ref.assertDataFrameCorrect(output, EXTRACT_EOUT)


def test_melt_year_built_columns(ref: ReferenceTest) -> None:
    """Melted columns match (tdda generated) reference file.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    statistics = pd.read_csv(EXTRACT_EOUT)

    output = sa_statistics._melt_year_built_columns(statistics)

    ref.assertDataFrameCorrect(output, MELT_EOUT)


def test_clean_year_built_columns(ref: ReferenceTest) -> None:
    """Cleaned columns match reference file.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    statistics = pd.read_csv(MELT_EOUT)

    output = sa_statistics._clean_year_built_columns(statistics)

    ref.assertDataFrameCorrect(output, CLEANED_EOUT)


def test_extract_dublin_small_areas() -> None:
    """Extracted Dublin small areas match (manually generated) file."""
    statistics = pd.read_csv(CLEANED_EOUT)
    geometries = gpd.read_parquet(SA_GEOMETRIES)

    output = sa_statistics._extract_dublin_small_areas(statistics, geometries)

    expected_output = gpd.read_parquet(STATS_EOUT)
    assert_geodataframe_equal(output, expected_output)


def test_link_small_areas_to_postcodes() -> None:
    """Small Areas that are 'mostly' in Postcode are linked to Postcode."""
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

    expected_output = gpd.GeoDataFrame(
        {
            "small_area": [1, 2],
            "geometry": [
                Polygon([(1, 0), (1, 1), (3, 1)]),
                Polygon([(1, 0), (1, 1), (0, 1)]),
            ],
            "postcodes": ["Co. Dublin", "Co. Dublin"],
        },
    )

    output = sa_statistics._link_small_areas_to_postcodes(small_areas, postcodes)

    assert_geodataframe_equal(output, expected_output)


def test_transform_sa_statistics() -> None:
    """Transformation pipeline of statistics matches reference file."""
    statistics = pd.read_csv(STATS_IN)
    glossary = pd.read_excel(GLOSSARY, engine="openpyxl")
    geometries = gpd.read_parquet(SA_GEOMETRIES)

    """DOES:
    - Extracts year built columns via glossary
    - Melts year built columns into rows for each year built type
    - Clean year built column:
        $ remove irrelevant substrings
        $ drop irrelevant columns
        $ rename columns
        $ set dtypes
    - Extract Dublin Small Areas
    - Link Small Areas to Postcodes
    """
    output = transform_sa_statistics.run(statistics, glossary, geometries)

    expected_output = gpd.read_parquet(STATS_EOUT)
    assert_geodataframe_equal(output, expected_output)
