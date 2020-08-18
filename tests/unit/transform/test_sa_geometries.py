from pathlib import Path

import geopandas as gpd

from geopandas.testing import assert_geodataframe_equal

import drem

from drem.extract.sa_geometries import _extract_dublin_local_authorities
from drem.filepaths import UTEST_DATA_TRANSFORM


SAS_IN: Path = UTEST_DATA_TRANSFORM / "sa_geometries_raw.parquet"
SAS_EOUT: Path = UTEST_DATA_TRANSFORM / "sa_geometries_clean.parquet"


def test_extract_dublin_local_authorities() -> None:
    """Extracted DataFrame contains only Dublin local authorities."""
    geometries = gpd.GeoDataFrame(
        {
            "COUNTYNAME": [
                "Kildare",
                "Dun Laoghaire-Rathdown",
                "Fingal",
                "South Dublin",
                "Dublin City",
            ],
        },
    )

    expected_output = gpd.GeoDataFrame(
        {
            "COUNTYNAME": [
                "Dun Laoghaire-Rathdown",
                "Fingal",
                "South Dublin",
                "Dublin City",
            ],
        },
    )

    output = _extract_dublin_local_authorities(geometries)

    assert_geodataframe_equal(output, expected_output)


def test_transform_sa_geometries() -> None:
    """Transformed CSO SA geometries match reference data."""
    geometries = gpd.read_parquet(SAS_IN)

    output = drem.transform_sa_geometries.run(geometries).reset_index(drop=True)

    expected_output = gpd.read_parquet(SAS_EOUT)
    assert_geodataframe_equal(output, expected_output, check_like=True)
