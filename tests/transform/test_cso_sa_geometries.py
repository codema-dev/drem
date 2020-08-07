from pathlib import Path

import geopandas as gpd

from geopandas.testing import assert_geodataframe_equal

import drem

from drem.filepaths import TEST_DIR


SAS_IN: Path = TEST_DIR / "data" / "cso_sa_geometries_in"
SAS_EOUT: Path = TEST_DIR / "data" / "cso_sa_geometries_eout"


def test_transform_cso_sa_geometries() -> None:
    """Transformed CSO SA geometries match reference file."""
    geometries = gpd.read_file(SAS_IN)
    output = drem.transform_cso_sa_geometries.run(geometries).reset_index(drop=True)
    expected_output = gpd.read_file(SAS_EOUT)
    assert_geodataframe_equal(output, expected_output, check_like=True)
