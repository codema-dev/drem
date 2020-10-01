import geopandas as gpd
import pandas as pd

from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Point

from drem.estimate.sa_demand import EstimateSmallAreaDemand


_estimate_small_area_demand = EstimateSmallAreaDemand()


def test_estimate_small_area_demand_matches_expected() -> None:
    """Total residential heat demand output matches expected output."""
    small_area_statistics = gpd.GeoDataFrame(
        {
            "small_area": [267112002, 267112002],
            "cso_period_built": ["1971 - 1980", "before 1919"],
            "households": [21, 10],
            "postcodes": ["Dublin 24", "Dublin 24"],
            "geometry": [Point((1, 1)), Point((1, 1))],
        },
    )
    ber_archetypes = pd.DataFrame(
        {
            "postcodes": ["Dublin 24", "Dublin 24"],
            "cso_period_built": ["1971 - 1980", "before 1919"],
            "mean_heat_demand_per_archetype": [15000, 20000],
        },
    )
    sa_geometries = gpd.GeoDataFrame(
        {
            "small_area": [267112002, 267112002],
            "geometry": [Point((1, 1)), Point((1, 1))],
        },
        crs="epsg:4326",
    )

    expected_output = gpd.GeoDataFrame(
        {
            "small_area": [267112002],
            "total_heat_demand_per_sa_kwh": [515000],
            "total_heat_demand_per_sa_gwh": [0.515],
            "geometry": [Point((1, 1))],
        },
        crs="epsg:4326",
    )

    output: gpd.GeoDataFrame = _estimate_small_area_demand.run(
        small_area_statistics, ber_archetypes, sa_geometries,
    )

    assert_geodataframe_equal(output, expected_output, check_like=True)
