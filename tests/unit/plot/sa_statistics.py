import geopandas as gpd
import pytest

from icontract import ViolationError
from shapely.geometry import Point

from drem.plot.sa_statistics import plot_sa_statistics


def test_plot_sa_statistics_raises_error() -> None:
    """Plot function raises error when doesn't recieve expected columns."""
    sa_statistics: gpd.GeoDataFrame = gpd.GeoDataFrame({"geometry": [Point(0, 0)]})

    with pytest.raises(ViolationError):
        plot_sa_statistics(sa_statistics)
