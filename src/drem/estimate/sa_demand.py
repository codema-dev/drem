from typing import Any

import geopandas as gpd
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception


@task
def _merge(gdf: gpd.GeoDataFrame, df: pd.DataFrame, **kwargs: Any) -> gpd.GeoDataFrame:

    return gdf.merge(df, **kwargs).drop_duplicates()


@task
def _multiply_columns(
    df: pd.DataFrame, left: str, right: str, result: str,
) -> pd.DataFrame:

    df[result] = df[left] * df[right]

    return df


@task
def _aggregate(df: pd.DataFrame, to: str, on: str, result: str) -> pd.DataFrame:

    return (
        df.pivot_table(index=on, values=[to], aggfunc="sum")
        .reset_index()
        .rename(columns={to: result})
    )


@task
def _divide_column(df: pd.DataFrame, target: str, result: str, by: int) -> pd.DataFrame:

    df[result] = df[target] / by

    return df


with Flow("Estimate Dublin Small Area Annual Energy Demands") as flow:

    sa_statistics = Parameter("sa_statistics")
    ber_archetypes = Parameter("ber_archetypes")
    sa_geometries = Parameter("sa_geometries")

    sa_linked_to_ber_archetypes = _merge(
        gdf=sa_statistics, df=ber_archetypes, on=["postcodes", "period_built"],
    )
    sa_with_total_heat_demand_column = _multiply_columns(
        sa_linked_to_ber_archetypes,
        left="households",
        right="mean_heat_demand_per_hh",
        result="total_heat_demand_per_archetype",
    )
    sa_demand_kwh = _aggregate(
        sa_with_total_heat_demand_column,
        on="small_area",
        to="total_heat_demand_per_archetype",
        result="total_heat_demand_per_sa_kwh",
    )
    sa_demand_gwh = _divide_column(
        sa_demand_kwh,
        target="total_heat_demand_per_sa_kwh",
        result="total_heat_demand_per_sa_gwh",
        by=10 ** 6,
    )
    sa_demand_with_geometries = _merge(df=sa_demand_gwh, gdf=sa_geometries)


class EstimateSmallAreaDemand(Task):
    """Estimate Small Area Energy Demand.

    Instantiates a prefect Task object to run this module's prefect Flow.
    see https://docs.prefect.io/core/concepts/flows.html

    Args:
        Task (Task): see https://docs.prefect.io/core/concepts/tasks.html#slug
    """

    def run(
        self,
        sa_statistics: gpd.GeoDataFrame,
        ber_archetypes: pd.DataFrame,
        sa_geometries: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Run EstimateSmallAreaDemand flow.

        Args:
            sa_statistics (gpd.GeoDataFrame): Clean Small Area Statistics
            ber_archetypes (pd.DataFrame): BER Archetypal Energy Averages
            sa_geometries (gpd.GeoDataFrame): Small Area Geometries

        Returns:
            gpd.GeoDataFrame: Small Area Energy Demand
        """
        with raise_on_exception():
            state = flow.run(
                parameters=dict(
                    sa_statistics=sa_statistics,
                    ber_archetypes=ber_archetypes,
                    sa_geometries=sa_geometries,
                ),
            )

        return state.result[sa_demand_with_geometries].result
