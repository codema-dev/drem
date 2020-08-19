from pathlib import Path

import geopandas as gpd
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from pandas.testing import assert_frame_equal
from shapely.geometry import Point

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform import vo
from drem.transform.vo import _convert_to_geodataframe
from drem.transform.vo import _merge_address_columns_into_one
from drem.transform.vo import _remove_symbols_from_column_strings


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


def test_remove_symbols_from_column_strings() -> None:
    """Remove symbols [,-] from column strings."""
    uses: pd.DataFrame = pd.DataFrame({"Uses": ["Office (House), -", "-, -"]})
    expected_output: pd.DataFrame = pd.DataFrame({"Uses": ["Office (House)", ""]})

    output: pd.DataFrame = _remove_symbols_from_column_strings(uses, "Uses")
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


def test_transform_vo() -> None:
    """Transformed Valuation Office GeoDataFrame matches reference file."""
    vo_raw: pd.DataFrame = pd.read_parquet(VO_IN)
    output = vo.transform_vo(vo_raw)
    expected_output: gpd.GeoDataFrame = gpd.read_parquet(VO_EOUT)
    assert_geodataframe_equal(output, expected_output)
