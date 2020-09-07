# flake8: noqa

from pathlib import Path

import geopandas as gpd

from icontract import require


@require(lambda demand_type: demand_type in {"fossil_fuel", "electricity"})
def plot_commercial_small_area_demand(
    vo: gpd.GeoDataFrame,
    sa_geometries: gpd.GeoDataFrame,
    demand_type: str,
    savedir: Path,
):
    """Plot Commercial Demand Estimate at Small Area level.

    Args:
        vo (gpd.GeoDataFrame): Valuation Office Dataset with demand estimates by
        building
        sa_geometries (gpd.GeoDataFrame): Small Area geometries
        demand_type (str): Demand type to be plotted; 'electricity' or 'fossil_fuel'
        savedir (Path): Path to save directory for plot
    """
    get_demand_type = {
        "fossil_fuel": "typical_fossil_fuel_demand",
        "electricity": "typical_electricity_demand",
    }

    entered_demand_type = get_demand_type[demand_type]

    demand_column_name = f"total_{demand_type}_demand"

    total_demand = (
        gpd.sjoin(sa_geometries, vo)
        .groupby("small_area")[entered_demand_type]
        .sum()
        .divide(1 * 10 ** 6)
        .rename(demand_column_name)
        .reset_index()
    )

    demand = sa_geometries.merge(total_demand, how="left")

    (
        demand.plot(
            column=demand_column_name,
            legend=True,
            legend_kwds={"label": f"Total Commercial {demand_type} Demand [GWh/year]"},
            figsize=(50, 25),
            missing_kwds={"color": "lightgrey", "label": "Missing values"},
        )
        .get_figure()
        .savefig(savedir / f"Commercial {demand_type} demand.png")
    )
