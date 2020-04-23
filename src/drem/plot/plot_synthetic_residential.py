import geopandas as gpd
import geoparquet as gpq
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from icontract import ViolationError, ensure, require
from pipeop import pipes
from prefect import Flow, task

from codema_drem._filepaths import (
    DATA_DIR,
    DUBLIN_EDS_CLEAN,
    PLOT_DIR,
    SYN_RESID_RESULTS,
    BER_CLEAN,
)
from codema_drem.utilities.flow import run_flow

ALLDUBLIN_ENERGY_RATINGS_PLOT = (
    PLOT_DIR / "maps" / "synthetic-residential-alldublin-energy-ratings.png"
)
BER_ENERGY_RATINGS_PLOT = PLOT_DIR / "maps" / "ber-only-alldublin-energy-ratings.png"

# ED_ENERGY_RATINGS = DATA_DIR / "results" / "ed_energy_ratings.parquet"
ED_ENERGY_RATINGS = DATA_DIR / "results" / "ed_energy_ratings"
ED_ENERGY_RATINGS_MAP_DIR = PLOT_DIR / "maps" / "ber_ratings"
ED_ENERGY_RATINGS_FLOW = PLOT_DIR / "flows" / "plot_ber_ratings"

ED_TOTAL_HD = DATA_DIR / "results" / "total_hd"
ED_TOTAL_HD_MAP = PLOT_DIR / "maps" / "ed_heat_demand.png"
ED_TOTAL_HD_FLOW = PLOT_DIR / "flows" / "plot_total_hd"


@pipes
def plot_ed_demands() -> Flow:

    with Flow("Get BER heat demands for eds") as flow:

        eds = _load_dublin_eds()
        synthetic_residential = _load_synthetic_residential()

        total_heat_demands = (
            _calculate_mean_heat_demands(synthetic_residential)
            >> _calculate_total_heat_demands
            >> _link_to_empty_eds(eds)
            >> _link_to_ed_geometries(eds)
            >> _replace_empty_values_with_zero
        )

        _save_total_heat_demand(total_heat_demands)
        _plot_total_heat_demand(total_heat_demands)

    return flow


@pipes
def plot_ed_ber_ratings() -> Flow:

    with Flow("Get BER rating freq for eds") as flow:

        eds = _load_dublin_eds()
        synthetic_residential = _load_synthetic_residential()

        energy_ratings = (
            _calculate_energy_rating_count_by_eds(synthetic_residential)
            >> _link_to_empty_eds(eds)
            >> _reindex_energy_rating_so_all_eds_have_same_index
            >> _link_to_ed_geometries(eds)
            >> _replace_empty_values_with_zero
        )

        _save_energy_ratings(energy_ratings)
        _plot_energy_ratings(energy_ratings, eds)

    return flow


# ---------------------------------------------------------------------------


@task
def _load_dublin_eds() -> gpd.GeoDataFrame:

    return gpd.read_file(DUBLIN_EDS_CLEAN)


@task
def _load_synthetic_residential() -> gpd.GeoDataFrame:

    return pd.read_parquet(SYN_RESID_RESULTS)


@task
def _load_ber() -> gpd.GeoDataFrame:

    return pd.read_parquet(BER_CLEAN)


@task
def _calculate_mean_heat_demands(buildings: pd.DataFrame) -> pd.DataFrame:

    buildings = buildings.copy()

    return (
        buildings.pivot_table(
            values=[
                "DeliveredEnergyMainSpace",
                "DeliveredEnergyMainWater",
                "DeliveredEnergySecondarySpace",
                "DeliveredEnergySupplementaryWater",
            ],
            index="eds",
            aggfunc="mean",
        )
        .reset_index()
        .rename(
            columns={
                "DeliveredEnergyMainSpace": "mean_main_sh",
                "DeliveredEnergyMainWater": "mean_main_hw",
                "DeliveredEnergySecondarySpace": "mean_sec_sh",
                "DeliveredEnergySupplementaryWater": "mean_sec_hw",
            }
        )
    )


@task
def _calculate_total_heat_demands(buildings: pd.DataFrame) -> pd.DataFrame:

    buildings = buildings.copy()

    buildings = buildings.set_index("eds")

    return (
        (
            buildings[["mean_main_sh", "mean_main_hw", "mean_sec_sh", "mean_sec_hw",]]
            .sum(axis="columns")
            .rename("total_heat_demand")
        )
        .divide(1 * 10 ** 3)  # NOTE: convert from kWh/year to MWh/year
        .reset_index()
    )


def _plot_ber_alldublin_energy_ratings(buildings: pd.DataFrame) -> None:

    plt.close()

    ax = (
        buildings["EnergyRating"]
        .value_counts()
        .divide(10 ** 3)
        .sort_index()
        .plot(kind="bar")
    )

    ax.set_title("[BER] All-of-Dublin Residential Energy ratings")
    ax.set_xlabel("BER Rating")
    ax.set_ylabel("1000s of buildings")

    # NOTE: https://stackoverflow.com/questions/10998621/rotate-axis-text-in-python-matplotlib
    ax.tick_params(axis="x", labelrotation=90)

    # NOTE: https://matplotlib.org/3.1.1/gallery/recipes/placing_text_boxes.html
    textstr = f"Total Number of Households = {len(buildings)}"
    props = dict(boxstyle="round", facecolor="wheat", alpha=0.5)
    ax.text(
        0.05,
        0.95,
        textstr,
        transform=ax.transAxes,
        fontsize=7,
        verticalalignment="top",
        bbox=props,
    )

    fig = ax.get_figure()
    fig.savefig(BER_ENERGY_RATINGS_PLOT)


def _plot_sr_alldublin_energy_ratings(buildings: pd.DataFrame) -> None:

    plt.close()

    ax = (
        buildings["EnergyRating"]
        .value_counts()
        .divide(10 ** 3)
        .sort_index()
        .plot(kind="bar")
    )

    ax.set_title("[Synthetic Residential] All-of-Dublin Residential Energy ratings")
    ax.set_xlabel("BER Rating")
    ax.set_ylabel("1000s of buildings")

    # NOTE: https://stackoverflow.com/questions/10998621/rotate-axis-text-in-python-matplotlib
    ax.tick_params(axis="x", labelrotation=90)

    # NOTE: https://matplotlib.org/3.1.1/gallery/recipes/placing_text_boxes.html
    textstr = f"Total Number of Households = {len(buildings)}"
    props = dict(boxstyle="round", facecolor="wheat", alpha=0.5)
    ax.text(
        0.05,
        0.95,
        textstr,
        transform=ax.transAxes,
        fontsize=7,
        verticalalignment="top",
        bbox=props,
    )

    fig = ax.get_figure()
    fig.savefig(ALLDUBLIN_ENERGY_RATINGS_PLOT)


@task
def _calculate_energy_rating_count_by_eds(buildings: pd.DataFrame) -> pd.DataFrame:

    return (
        buildings.groupby(["eds", "EnergyRating"], sort=False)["EnergyRating"]
        .count()
        .rename("frequencies")
    ).reset_index()


@task
def _link_to_ed_geometries(
    buildings: pd.DataFrame, dublin_eds: pd.DataFrame
) -> gpd.GeoDataFrame:

    # NOTE: some electoral districts are empty as there are strangely
    # no buildings registered in the census for them...

    return dublin_eds.merge(buildings, how="left")


@task
def _link_to_empty_eds(
    aggregated_buildings: pd.DataFrame, dublin_eds: pd.DataFrame
) -> pd.DataFrame:

    return pd.merge(dublin_eds["eds"], aggregated_buildings, how="left")


@task
def _reindex_energy_rating_so_all_eds_have_same_index(
    building_ratings: pd.DataFrame,
) -> pd.DataFrame:

    building_ratings = building_ratings.copy()

    # NOTE: https://pandas.pydata.org/pandas-docs/stable/user_guide/advanced.html
    eds_unique = building_ratings["eds"].unique()
    energy_ratings_unique = building_ratings["EnergyRating"].cat.categories.values

    new_index = pd.MultiIndex.from_product(
        [eds_unique, energy_ratings_unique], names=["eds", "EnergyRating"]
    )

    building_ratings = building_ratings.set_index(["eds", "EnergyRating"])

    return building_ratings.reindex(new_index).sort_index().reset_index()


@task
def _replace_empty_values_with_zero(
    energy_ratings: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return energy_ratings.fillna(0)


@task
@require(lambda energy_ratings: "frequencies" in energy_ratings.columns)
def _plot_energy_ratings(
    energy_ratings: gpd.GeoDataFrame, eds: gpd.GeoDataFrame
) -> None:

    for rating in energy_ratings["EnergyRating"].unique():

        plt.close()

        find_rows_matching_rating = energy_ratings["EnergyRating"] == rating
        rating_data = energy_ratings[find_rows_matching_rating]

        fig = (
            rating_data.plot(column="frequencies", legend=True)
            .set_title(rating)
            .get_figure()
        )

        fig.savefig(ED_ENERGY_RATINGS_MAP_DIR / f"{rating}.png")


@task
@require(lambda total_heat_demand: "total_heat_demand" in total_heat_demand.columns)
def _plot_total_heat_demand(total_heat_demand: gpd.GeoDataFrame) -> None:

    plt.close()
    fig = (
        total_heat_demand.plot(column="total_heat_demand", legend=True)
        .set_title("Total Heat Demand [MWh/year]")
        .get_figure()
    )

    fig.savefig(ED_TOTAL_HD_MAP)


@task
def _save_energy_ratings(energy_ratings: gpd.GeoDataFrame,) -> None:

    # gpq.to_geoparquet(energy_ratings, ED_ENERGY_RATINGS)
    energy_ratings.to_file(ED_ENERGY_RATINGS)


@task
def _save_total_heat_demand(total_heat_demand: gpd.GeoDataFrame,) -> None:

    # gpq.to_geoparquet(energy_ratings, ED_TOTAL_HD)
    total_heat_demand.to_file(ED_TOTAL_HD)
