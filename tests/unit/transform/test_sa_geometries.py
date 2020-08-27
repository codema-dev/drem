from pathlib import Path

import geopandas as gpd
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from pandas.testing import assert_frame_equal

import drem

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.sa_geometries import extract_dublin_local_authorities


SAS_IN: Path = UTEST_DATA_TRANSFORM / "sa_geometries_raw.parquet"
SAS_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_sa_geometries_clean.parquet"


def test_extract_dublin_local_authorities() -> None:
    """Extracted DataFrame contains only Dublin local authorities."""
    geometries = pd.DataFrame(
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

    expected_output = pd.DataFrame(
        {
            "COUNTYNAME": [
                "Dun Laoghaire-Rathdown",
                "Fingal",
                "South Dublin",
                "Dublin City",
            ],
        },
    )

    output = extract_dublin_local_authorities(geometries).reset_index(drop=True)

    assert_frame_equal(
        output, expected_output,
    )


def test_transform_sa_geometries() -> None:
    """Transformed CSO SA geometries match reference data."""
    geometries = gpd.read_parquet(SAS_IN)

    output = drem.transform_sa_geometries.run(geometries).reset_index(drop=True)

    expected_output = gpd.read_parquet(SAS_EOUT)
    assert_geodataframe_equal(output, expected_output, check_like=True)
