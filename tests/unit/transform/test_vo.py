from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from pandas.testing import assert_frame_equal
from shapely.geometry import Point

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.vo import _convert_to_geodataframe
from drem.transform.vo import _extract_use_from_vo_uses_column
from drem.transform.vo import _merge_address_columns_into_one
from drem.transform.vo import _merge_benchmarks_into_vo


VO_IN: Path = UTEST_DATA_TRANSFORM / "vo_raw.parquet"
VO_EOUT: Path = UTEST_DATA_TRANSFORM / "vo_clean.parquet"


def test_merge_address_columns_into_one() -> None:
    """Merge string columns into one."""
    addresses: pd.DataFrame = pd.DataFrame(
        {"Address 1": ["7 Rowanbyrne"], "Address 2": ["Blackrock"]},
    )

    expected_output: pd.DataFrame = pd.DataFrame(
        {
            "Address 1": ["7 Rowanbyrne"],
            "Address 2": ["Blackrock"],
            "Address": ["7 Rowanbyrne Blackrock"],
        },
    )

    output: pd.DataFrame = _merge_address_columns_into_one(addresses)
    assert_frame_equal(output, expected_output)


def test_extract_use_from_vo_uses_column() -> None:
    """Split 'Uses' column into multiple use columns."""
    vo = pd.DataFrame({"Uses": ["KIOSK, CLOTHES SHOP", "WAREHOUSE, -"]})
    expected_output = pd.DataFrame(
        {
            "Uses": ["KIOSK, CLOTHES SHOP", "WAREHOUSE, -"],
            "use_0": ["KIOSK", "WAREHOUSE"],
            "use_1": ["CLOTHES SHOP", np.nan],
        },
    )

    output: pd.DataFrame = _extract_use_from_vo_uses_column(vo)

    assert_frame_equal(output, expected_output)


def test_merge_benchmarks_into_vo() -> None:
    """Merge benchmarks into vo data."""
    benchmark = pd.DataFrame(
        {
            "benchmark": [
                "Bar, Pub or Licensed Club (TM:46)",
                "Catering: Fast Food Restaurant",
                "Catering: Fast Food Restaurant",
            ],
            "vo_use": ["PUB", "TAKE AWAY", "RESTAURANT TAKE AWAY"],
            "demand": [350, 130, 130],
        },
    )

    vo = pd.DataFrame(
        {"use_0": ["PUB", "RESTAURANT TAKE AWAY"], "use_1": [np.nan, "TAKE AWAY"]},
    )

    expected_output = pd.DataFrame(
        {
            "use_0": ["PUB", "RESTAURANT TAKE AWAY"],
            "use_1": [np.nan, "TAKE AWAY"],
            "benchmark": [
                "Bar, Pub or Licensed Club (TM:46)",
                "Catering: Fast Food Restaurant",
            ],
            "vo_use": ["PUB", "RESTAURANT TAKE AWAY"],
            "demand": [350, 130],
        },
    )

    output: pd.DataFrame = _merge_benchmarks_into_vo(vo, benchmark)

    assert_frame_equal(output, expected_output)


def test_convert_to_geodataframe() -> None:
    """Convert DataFrame to GeoDataFrame."""
    coordinates: pd.DataFrame = pd.DataFrame(
        {"X ITM": [717205.47], "Y ITM": [733589.23]},
    )

    expected_output: gpd.GeoDataFrame = gpd.GeoDataFrame(
        coordinates, geometry=[Point(717205.47, 733589.23)], crs="epsg:2157",
    )

    output: gpd.GeoDataFrame = _convert_to_geodataframe(coordinates)
    assert_geodataframe_equal(output, expected_output)
