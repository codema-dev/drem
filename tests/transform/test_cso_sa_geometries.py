import geopandas as gpd

from geopandas.testing import assert_geodataframe_equal

import drem

from drem._filepaths import TEST_DIR


SAS_IN = TEST_DIR / "transform" / "test_data" / "cso_sa_geometries_in"
SAS_EOUT = TEST_DIR / "transform" / "test_data" / "cso_sa_geometries_eout"


def test_transform_cso_sa_geometries() -> None:
    """Transformed CSO SA geometries match reference file."""
    output = drem.transform_cso_sa_geometries.run(SAS_IN).reset_index()
    expected_output = gpd.read_file(SAS_EOUT)
    assert_geodataframe_equal(output, expected_output, check_like=True)
