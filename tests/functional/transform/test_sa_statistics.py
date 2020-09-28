import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from geopandas.testing import assert_geodataframe_equal
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception
from shapely.geometry import Polygon

from drem.transform.sa_statistics import TransformSaStatistics
from drem.transform.sa_statistics import clean_year_built
from drem.transform.sa_statistics import flow


transform_sa_statistics = TransformSaStatistics()


@pytest.fixture
def raw_sa_glossary() -> pd.DataFrame:
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
                "Flat/Apartment (No. of persons)",
                "Pre 1919 (No. of households)",
                "1919 - 1945 (No. of persons)",
                "No central heating",
                "Oil",
            ],
        },
    )


@pytest.fixture
def raw_sa_statistics() -> pd.DataFrame:
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


@pytest.fixture
def dublin_sa_geometries() -> gpd.GeoDataFrame:
    """Create Dublin Small Area Geometries.

    Returns:
        gpd.GeoDataFrame: Dublin Small Area Geometries
    """
    return gpd.GeoDataFrame(
        {
            "small_area": ["017001001"],
            "geometry": [Polygon([(0, 0), (0, 0.5), (0.5, 0)])],
        },
    )


@pytest.fixture
def dublin_postcodes() -> gpd.GeoDataFrame:
    """Create Dublin Postcodes.

    Returns:
        gpd.GeoDataFrame: Dublin Postcodes
    """
    return gpd.GeoDataFrame(
        {"postcodes": ["Co. Dublin"], "geometry": [Polygon([(0, 0), (0, 1), (1, 0)])]},
    )


@pytest.fixture
def flow_state(
    raw_sa_glossary: pd.DataFrame,
    raw_sa_statistics: pd.DataFrame,
    dublin_postcodes: gpd.GeoDataFrame,
    dublin_sa_geometries: gpd.GeoDataFrame,
) -> State:
    """Run etl flow with dummy test data.

    Args:
        raw_sa_glossary (pd.DataFrame): Raw glossary table
        raw_sa_statistics (pd.DataFrame): Raw Ireland Small Area Statistics
        dublin_postcodes (gpd.GeoDataFrame): Dublin Postcodes
        dublin_sa_geometries (gpd.GeoDataFrame): Dublin Small Area Geometries

    Returns:
        State: A Prefect State object containing flow run information
    """
    with raise_on_exception():
        state = flow.run(
            dict(
                raw_sa_glossary=raw_sa_glossary,
                raw_sa_stats=raw_sa_statistics,
                dublin_pcodes=dublin_postcodes,
                dublin_sa_geom=dublin_sa_geometries,
            ),
        )

    return state


def test_no_transform_sa_stats_tasks_fail(flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        flow_state (State): A Prefect State object containing flow run information
    """
    assert flow_state.is_successful()


def test_transform_year_built_matches_expected(flow_state: State) -> None:
    """Transform year built data matches expected.

    Args:
        flow_state (State): A Prefect State object containing flow run information
    """
    expected_output = gpd.GeoDataFrame(
        {
            "small_area": ["017001001", "017001001"],
            "postcodes": ["Co. Dublin", "Co. Dublin"],
            "period_built": ["Pre 1919", "1919 - 1945"],
            "households": [10, np.nan],
            "persons": [np.nan, 20],
            "geometry": [
                Polygon([(0, 0), (0, 0.5), (0.5, 0)]),
                Polygon([(0, 0), (0, 0.5), (0.5, 0)]),
            ],
        },
    )

    output = flow_state.result[clean_year_built].result

    assert_geodataframe_equal(output, expected_output)
