import pytest
import pandas as pd
import geopandas as gpd

from shapely.geometry import Polygon

import src.preprocess.Dublin_EDs as testing


@pytest.fixture
def dublin_eds_clean() -> gpd.GeoDataFrame:

    return testing.data_pipeline()


def test_get_area_of_each_geometry(dublin_eds_clean):

    p1 = Polygon([(0, 0), (1, 0), (1, 1)])          # p1.area = 0.5
    p2 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # p2.area = 1
    p3 = Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])  # p3.area = 1

    gdf = gpd.GeoDataFrame([p1, p2, p3])

    gdf['Area'] = testing._get_area_of_each_geometry(gdf)

    assert gdf['Area'] == pd.Series([0.5, 1, 1])
