from pathlib import Path

import geopandas as gpd
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from tdda.referencetest.referencetest import ReferenceTest

from drem import transform_sa_statistics
from drem.filepaths import TEST_DATA_TRANSFORM
from drem.transform.sa_statistics import _clean_year_built_columns
from drem.transform.sa_statistics import _extract_dublin_small_areas
from drem.transform.sa_statistics import _extract_year_built
from drem.transform.sa_statistics import _melt_year_built_columns


STATS_IN: Path = TEST_DATA_TRANSFORM / "sa_statistics_raw.csv"
GLOSSARY: Path = TEST_DATA_TRANSFORM / "sa_glossary.xlsx"
GEOMETRIES: Path = TEST_DATA_TRANSFORM / "dublin_sa_geometries_clean.parquet"

EXTRACT_EOUT: Path = TEST_DATA_TRANSFORM / "dublin_sa_extract_year_built.csv"
MELT_EOUT: Path = TEST_DATA_TRANSFORM / "dublin_sa_melt_year_built.csv"
CLEANED_EOUT: Path = TEST_DATA_TRANSFORM / "dublin_sa_clean_year_built.csv"
STATS_EOUT: Path = TEST_DATA_TRANSFORM / "dublin_sa_statistics_clean.parquet"


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


def test_extract_dublin_small_areas() -> None:
    """Extracted Dublin small areas match (manually generated) file."""
    statistics = pd.read_csv(CLEANED_EOUT)
    geometries = gpd.read_parquet(GEOMETRIES)

    output = _extract_dublin_small_areas(statistics, geometries)

    expected_output = gpd.read_parquet(STATS_EOUT)
    assert_geodataframe_equal(output, expected_output)


def test_transform_sa_statistics() -> None:
    """Transformation pipeline of statistics matches (manually generated) reference file."""
    statistics = pd.read_csv(STATS_IN)
    glossary = pd.read_excel(GLOSSARY, engine="openpyxl")
    geometries = gpd.read_parquet(GEOMETRIES)

    output = transform_sa_statistics.run(statistics, glossary, geometries)

    expected_output = gpd.read_parquet(STATS_EOUT)
    assert_geodataframe_equal(output, expected_output)
