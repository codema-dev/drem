import pandas as pd
import geopandas as gpd
import pandas as pd
import numpy as np
import icontract
from prefect import Flow, task

from pipeop import pipes
from typing import Tuple
from codema_drem.utilities.paths import BASE_DIR, DATA_DIR, PLOT_DIR
from codema_drem.utilities.logging import create_logger, log_df
from codema_drem.utilities.plotting import plot_geodf_to_file

LOGGER = create_logger(caller=__name__)
ARCHETYPE_FNAME = "archetype_model"
BER_FNAME = "ber_clean.pkl"
CENSUS_FNAME = "census2016_clean.pkl"
DUBLIN_EDS_FNAME = "dublin_eds"
TOTAL_NUMBER_OF_EDs = 318


@pipes
def archetype_etl_flow() -> pd.DataFrame:

    with Flow("archetype-creation") as flow:
        # Get average archetype demand for each postcode:
        pcode_avgs = _load_ber() >> _calc_avg_postcode_demands
        dublin_avgs = _load_ber() >> _calc_avg_all_of_dublin_demands
        avg_archetype_demand_for_each_postcode = _replace_avgs_in_sample_less_than_twenty_with_all_of_dublin(
            pcode_avgs=pcode_avgs, dublin_avgs=dublin_avgs,
        )

        ber_avgs = (
            avg_archetype_demand_for_each_postcode
            >> _reset_index
            >> _convert_object_cols_to_strings_for_merging
        )
        dublin_eds = _load_dublin_eds()
        ber_avgs_with_eds = _link_ber_postcode_avgs_to_eds(
            ber_avgs=ber_avgs, dublin_eds=dublin_eds,
        )

        census = _load_census()
        electoral_district_totals = (
            _link_ber_postcode_avgs_to_census(
                ber_avgs=ber_avgs_with_eds, census=census,
            )
            >> _estimate_total_ed_demands_using_ber_averages
            >> _aggregate_totals_to_electoral_districts
        )

        electoral_district_totals_linked_to_map = (
            _link_merged_df_to_ed_geometries(
                df=electoral_district_totals, ed_geometries=dublin_eds,
            )
            >> _convert_merged_df_to_geodf
            >> _convert_demand_units_to_gwh
            >> _archetype_model_complete
            >> _save_archetype_results
            >> _plot_archetype_demands
        )

    return flow


# @pipes
# def merge_census_and_ber_via_archetypes_and_save_results() -> None:

#     merged_df=_merge_census_and_ber_and_estimate_total_demands()
#     merged_df.to_file(DATA_DIR/'interim'/ARCHETYPE_FNAME)


# -----------------------------------------------------------


def not_missing_any_electoral_districts(df):

    return df.EDs.nunique() == TOTAL_NUMBER_OF_EDs


@task
def _load_ber() -> pd.DataFrame:

    path = DATA_DIR / "interim" / BER_FNAME
    return pd.read_pickle(path)


@task
def _load_census() -> pd.DataFrame:

    path = DATA_DIR / "interim" / CENSUS_FNAME
    return pd.read_pickle(path)


@task
def _load_dublin_eds() -> gpd.GeoDataFrame:

    path = DATA_DIR / "interim" / DUBLIN_EDS_FNAME
    return gpd.read_file(path)


@task
def _calc_avg_postcode_demands(ber: pd.DataFrame) -> pd.DataFrame:

    return (
        ber.reset_index()
        .pivot_table(
            index=["Postcodes", "Dwelling_Type", "Period_Built",],
            aggfunc={
                "DeliveredEnergyMainSpace": np.mean,
                "DeliveredEnergyMainWater": np.mean,
                "Postcodes": "count",
            },
        )
        .rename(
            columns={
                "DeliveredEnergyMainSpace": "avg_sh",
                "DeliveredEnergyMainWater": "avg_hw",
                "Postcodes": "sample_size",
            }
        )
    )


@task
def _calc_avg_all_of_dublin_demands(ber: pd.DataFrame) -> pd.DataFrame:

    return (
        ber.reset_index()
        .pivot_table(
            index=["Postcodes", "Dwelling_Type", "Period_Built",],
            aggfunc={
                "DeliveredEnergyMainSpace": np.mean,
                "DeliveredEnergyMainWater": np.mean,
            },
        )
        .rename(
            columns={
                "DeliveredEnergyMainSpace": "avg_sh",
                "DeliveredEnergyMainWater": "avg_hw",
            }
        )
    )


@task
def _replace_avgs_in_sample_less_than_twenty_with_all_of_dublin(
    pcode_avgs: pd.DataFrame, dublin_avgs: pd.DataFrame,
) -> pd.DataFrame:

    minimum_sample_size = 20

    pcode_avgs["postcode_specific_data"] = True

    # Set all averages with <20 samples to empty & track rows
    mask = pcode_avgs["sample_size"] < minimum_sample_size
    pcode_avgs.loc[mask, "avg_sh"] = np.nan
    pcode_avgs.loc[mask, "avg_hw"] = np.nan
    pcode_avgs.loc[mask, "postcode_specific_data"] = False

    # Replace all <20 samples with all-of-dublin avgs
    return pcode_avgs.combine_first(dublin_avgs)


@task
def _reset_index(df: pd.DataFrame,) -> pd.DataFrame:

    return df.reset_index()


@task
@icontract.ensure(lambda result: result.EDs.nunique() == TOTAL_NUMBER_OF_EDs)
def _link_ber_postcode_avgs_to_eds(
    ber_avgs: pd.DataFrame, dublin_eds: gpd.GeoDataFrame
) -> pd.DataFrame:

    return pd.merge(ber_avgs, dublin_eds)


@task
def _convert_object_cols_to_strings_for_merging(df: pd.DataFrame,) -> pd.DataFrame:

    return df.convert_dtypes()


@task
@icontract.ensure(lambda result: result.EDs.nunique() == TOTAL_NUMBER_OF_EDs)
def _link_ber_postcode_avgs_to_census(
    ber_avgs: gpd.GeoDataFrame, census: pd.DataFrame,
) -> pd.DataFrame:

    return pd.merge(ber_avgs, census, on=["EDs", "Dwelling_Type", "Period_Built"])


@task
def _estimate_total_ed_demands_using_ber_averages(df: pd.DataFrame) -> pd.DataFrame:

    df["total_hw"] = df["avg_hw"] * df["Total_HH"]
    df["total_sh"] = df["avg_sh"] * df["Total_HH"]

    return df


@task
def _aggregate_totals_to_electoral_districts(df: pd.DataFrame) -> pd.DataFrame:

    return df.pivot_table(
        values=["total_hw", "total_sh"], index="EDs", aggfunc=np.sum,
    ).reset_index()


@task
def _link_merged_df_to_ed_geometries(
    df: pd.DataFrame, ed_geometries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return pd.merge(df, ed_geometries[["geometry", "EDs"]], on="EDs")


@task
def _convert_merged_df_to_geodf(merged_df: pd.DataFrame) -> gpd.GeoDataFrame:

    return gpd.GeoDataFrame(merged_df, crs="epsg:4326")


@task
def _convert_demand_units_to_gwh(merged_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    demand_types = ["total_hw", "total_sh"]

    merged_df[demand_types] = merged_df[demand_types] / 10 ** 6

    return merged_df


@task(name="Archetype model complete")
def _archetype_model_complete(merged_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return merged_df


@task
def _save_archetype_results(merged_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    merged_df.to_excel(DATA_DIR / "results" / "archetype_demands.xlsx")

    return merged_df


@task(name="Plot Archetype Demands")
def _plot_archetype_demands(merged_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    demand_types = ["total_hw", "total_sh"]
    demand_full_names = {
        "total_hw": "Hot Water",
        "total_sh": "Space Heat",
    }

    for demand_type in demand_types:

        save_path = PLOT_DIR / f"archetype_{demand_type}_demand.png"

        properties = {
            "title": f"""Total Residential {demand_full_names[demand_type]}\n
            Demand per Electoral District [GWh/year]""",
            "xlabel": "Longitude",
            "ylabel": "Latitude",
        }

        ax = merged_df.plot(column=demand_type, legend=True)
        ax.set(**properties)
        ax.get_figure().savefig(save_path)

    return merged_df
