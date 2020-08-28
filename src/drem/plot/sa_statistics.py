import geopandas as gpd

from icontract import require


@require(lambda sa_statistics: "total_heat_demand_per_sa" in sa_statistics.columns)
def plot_sa_statistics(sa_statistics: gpd.GeoDataFrame) -> None:
    """Plot Small Area total heat demand via matplotlib.

    Args:
        sa_statistics (gpd.GeoDataFrame): Data on Small Area Heat Demands
    """
    sa_statistics.plot(
        column="total_heat_demand_per_sa",
        legend=True,
        legend_kwds={"label": "Residential Energy Demand [GWh/year]"},
        figsize=(50, 50),
    )
