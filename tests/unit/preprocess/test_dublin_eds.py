import pytest
import pandas as pd
import geopandas as gpd

from shapely.geometry import Polygon

from codema_drem.preprocess.Dublin_EDs import (
    _get_area_of_each_geometry,
    run_electoral_district_etl_flow,
)


def test_get_area_of_each_geometry(dublin_eds_clean):

    p1 = Polygon([(0, 0), (1, 0), (1, 1)])          # p1.area = 0.5
    p2 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # p2.area = 1
    p3 = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])  # p3.area = 1

    gdf = gpd.GeoDataFrame([p1, p2, p3])

    gdf['Area'] = _get_area_of_each_geometry.run(gdf)

    assert gdf['Area'] == pd.Series([0.5, 1, 1])


@pytest.mark.slow
def test_flow_run_is_successful() -> None:

    state, _ = run_electoral_district_etl_flow()
    assert state.is_successful()
