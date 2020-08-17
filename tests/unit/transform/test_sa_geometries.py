from pathlib import Path

import geopandas as gpd

from geopandas.testing import assert_geodataframe_equal

import drem

from drem.filepaths import TEST_DATA_TRANSFORM


SAS_IN: Path = TEST_DATA_TRANSFORM / "sa_geometries_raw.parquet"
SAS_EOUT: Path = TEST_DATA_TRANSFORM / "dublin_sa_geometries_clean.parquet"


def test_transform_sa_geometries() -> None:
    """Transformed CSO SA geometries match (manually generated) reference file."""
    geometries = gpd.read_parquet(SAS_IN)

    output = drem.transform_sa_geometries.run(geometries).reset_index(drop=True)

    expected_output = gpd.read_parquet(SAS_EOUT)
    assert_geodataframe_equal(output, expected_output, check_like=True)
