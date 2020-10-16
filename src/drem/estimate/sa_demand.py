from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception

import drem.utilities.geopandas_tasks as gpdt
import drem.utilities.pandas_tasks as pdt


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

    sa_periods_built_fpath = Parameter("sa_periods_built_fpath")
    ber_archetype_averages_fpath = Parameter("ber_archetype_averages_fpath")
    sa_geometries_fpath = Parameter("sa_geometries_fpath")

    sa_periods_built = gpdt.read_parquet(sa_periods_built_fpath)
    ber_archetype_averages = pdt.read_parquet(ber_archetype_averages_fpath)
    sa_geometries = gpdt.read_parquet(sa_geometries_fpath)

    sa_linked_to_ber_archetypes = _merge(
        gdf=sa_periods_built,
        df=ber_archetype_averages,
        on=["postcodes", "cso_period_built"],
    )
    sa_with_total_heat_demand_column = _multiply_columns(
        sa_linked_to_ber_archetypes,
        left="households",
        right="mean_heat_demand_per_archetype",
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
        small_area_period_built_filepath: Path,
        ber_archetypes_filepath: Path,
        small_area_geometries_filepath: Path,
        output_filepath: Path,
    ) -> gpd.GeoDataFrame:
        """Run EstimateSmallAreaDemand flow.

        Args:
            small_area_period_built_filepath (Path): Path to Clean Small Area Period
                Built Data
            ber_archetypes_filepath (Path): Path to BER Archetypal Energy Averages
            small_area_geometries_filepath (Path): Path to Clean Small Area Geometries
            output_filepath (Path): Path to a Small Area Demand Estimate
        """
        with raise_on_exception():
            state = flow.run(
                parameters=dict(
                    sa_periods_built_fpath=small_area_period_built_filepath,
                    ber_archetype_averages_fpath=ber_archetypes_filepath,
                    sa_geometries_fpath=small_area_geometries_filepath,
                ),
            )

        result = state.result[sa_demand_with_geometries].result

        result.to_parquet(output_filepath)


estimate_sa_demand = EstimateSmallAreaDemand()
