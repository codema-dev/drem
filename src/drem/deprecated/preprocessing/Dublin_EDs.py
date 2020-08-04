from pathlib import Path
from shutil import rmtree
from typing import List, Tuple

import geopandas as gpd  # to read/write spatial data
import matplotlib.pyplot as plt
import pandas as pd
from icontract import ensure
from pipeop import pipes
from toolz.functoolz import pipe
from prefect import Flow, task

from codema_drem.utilities.preprocessing import (
    convert_string_columns_to_categorical,
    infer_column_data_types,
    set_string_columns_to_lowercase,
)
from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
    # Inputs
    DUBLIN_EDS_RAW,
    DUBLIN_POSTCODES_CLEAN,
    # Outputs
    DUBLIN_EDS_CLEAN,
    DUBLIN_EDS_PLOT,
    DUBLIN_EDS_FLOW,
)
from codema_drem.utilities.flow import run_flow

# INTERMEDIATE RESULTS
# Save some intermediate results to avoid long runtime for reruns ...
EDS_OVERLAYED_ONTO_POSTCODES = DATA_DIR / "interim" / "eds_overlayed_onto_postcodes"


# Tasks
# *****

# Load Data
# ---------


@task
def _load_geodataframe(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def _set_coordinate_reference_system_to_wgs(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:  #

    gdf.crs = "epsg:4326"

    return gdf


@task
def _load_ireland_eds() -> gpd.GeoDataFrame:

    return gpd.read_file(DUBLIN_EDS_RAW, crs="epsg:4326")


@task
def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    return gpd.read_file(POSTCODES_CLEAN, crs="epsg:4326")


# Pull out Dublin
# ---------------


@task
def _pull_out_dublin_data(ireland_eds: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    mask = ireland_eds["COUNTY"] == "DUBLIN"
    return ireland_eds[mask]


@task
def _pull_out_columns(dublin_eds: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_eds[["ED_ENGLISH", "geometry"]]


# Reformat Data
# -------------


def _rename_columns(dublin_eds: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_eds.rename({"ED_ENGLISH": "eds"}, axis=1)


def _make_electoral_district_strings_lowercase(
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return dublin_eds.assign(eds=dublin_eds["eds"].str.lower())


def _reset_index(dublin_eds: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_eds.reset_index(drop=True)


@task
def _reformat_data(dublin_eds: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return pipe(
        _rename_columns(dublin_eds),
        _make_electoral_district_strings_lowercase,
        _reset_index,
    )


# Rename eds...
# -------------


@task
def _rename_electoral_districts_so_same_as_cso_map(df: pd.DataFrame,) -> pd.DataFrame:

    df["eds"] = (
        df["eds"]
        .str.replace(pat=r"(')", repl="")  # replace accents with blanks
        .str.replace("saint kevins", "st. kevins", regex=False)
        .str.replace(
            "dun laoghaire sallynoggin east",
            "dun laoghaire-sallynoggin east",
            regex=False,
        )
        .str.replace(
            "dun laoghaire sallynoggin south",
            "dun laoghaire-sallynoggin south",
            regex=False,
        )
    )

    return df


@task
def _get_overlapping_geometries_between_eds_and_pcodes(
    dublin_eds: gpd.GeoDataFrame, postcodes: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """The result of gpd.overlay identity consists of the surface of df1,
       but with the geometries obtained from overlaying df1 with df2
       """

    # Store result to intermediate file for faster runtimes
    if EDS_OVERLAYED_ONTO_POSTCODES.exists():
        eds_overlayed_onto_postcodes = gpd.read_file(EDS_OVERLAYED_ONTO_POSTCODES)
    else:
        """NOTE
            see https://geopandas.org/set_operations.html?highlight=overlay"""
        eds_overlayed_onto_postcodes = gpd.overlay(
            df1=dublin_eds, df2=postcodes, how="union",
        )
        eds_overlayed_onto_postcodes.to_file(EDS_OVERLAYED_ONTO_POSTCODES)

    return eds_overlayed_onto_postcodes


@task
def _get_area_of_each_geometry(merged_gdf: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return merged_gdf.assign(
        area=merged_gdf["geometry"].apply(lambda polygon: polygon.area)
    )


@task
# @ensure(lambda result:
#         any(result.isna().any()) is False,
#         error=lambda result:
#         ValueError("Following 'True' columns contain null values:\n\n"
#                    f'{result.isna().any()}'))
def _get_postcode_with_largest_geometric_area_overlap_for_each_ed(
    merged_gdf: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """The largest area within each ED represents largest
       overlap with the Postcode geometry.
       """

    eds_with_largest_overlap = (
        merged_gdf.groupby("eds")["area"].nlargest(1).reset_index().set_index("level_1")
    )

    largest_eds = merged_gdf.loc[
        eds_with_largest_overlap.index, ("eds", "postcodes")
    ].reset_index(drop=True)

    # NOTE - largest_eds is now a 2 column DataFrame matching eds to postcodes!
    return largest_eds


@task
def _infer_column_data_types_to_enable_merging(df: pd.DataFrame,) -> pd.DataFrame:

    return df.convert_dtypes()


@task
def _link_eds_to_postcodes(
    ed_postcode_link: pd.DataFrame, dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return dublin_eds.merge(ed_postcode_link)


@task
@ensure(
    lambda result: any(result.isna().any()) is False,
    error=lambda result: ValueError(
        "All columns marked 'True' contain null values:\n\n" f"{result.isna().any()}"
    ),
)
def _link_failed_matches_to_postcodes_with_sjoin(
    dublin_eds: gpd.GeoDataFrame, postcodes: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """The above overlay operation fails for Rush & Kilbarrack so these
    two Electoral Districts will be merged using sjoin

    Parameters
    ----------
    dublin_eds : gpd.GeoDataFrame
    postcodes : gpd.GeoDataFrame

    Returns
    -------
    gpd.GeoDataFrame
        dublin_eds with all eds linked to postcodes
    """

    empty_postcode_rows = dublin_eds.postcodes.isna()
    dublin_eds.loc[empty_postcode_rows, "postcodes"] = gpd.sjoin(
        dublin_eds[empty_postcode_rows], postcodes,
    )["postcodes_right"].unique()

    return dublin_eds


@task
def _save_geodataframe_result(dublin_eds: gpd.GeoDataFrame) -> None:

    dublin_eds.to_file(DUBLIN_EDS_CLEAN)


@task
def _annotate_and_plot_result(dublin_eds: gpd.GeoDataFrame,) -> None:

    columns_to_plot = ["postcodes", "eds"]

    for column in columns_to_plot:

        ax = dublin_eds.plot(
            column=column, categorical=True, legend=True, figsize=(15, 15),
        )
        properties = {
            "title": "Dublin Electoral Districts linked to Postcodes",
            "xlabel": "Longitude",
            "ylabel": "Latitude",
        }
        ax.set(**properties)

        fig = ax.get_figure()
        fig.savefig(Path(str(DUBLIN_EDS_PLOT) + "_column_" + column))
        plt.close(fig)


# Flows
# *****


@pipes
def dublin_eds_etl_flow() -> Flow:

    with Flow("Preprocess Dublin Electoral Districts") as flow:

        dublin_eds = (
            _load_geodataframe(DUBLIN_EDS_RAW)
            >> _pull_out_dublin_data
            >> _pull_out_columns
            >> _reformat_data
            >> _rename_electoral_districts_so_same_as_cso_map
        )

        # Link to Postcodes...
        postcodes = _load_geodataframe(DUBLIN_POSTCODES_CLEAN)

        eds_overlayed_onto_postcodes = _get_overlapping_geometries_between_eds_and_pcodes(
            dublin_eds=dublin_eds, postcodes=postcodes,
        )

        largest_postcode_in_each_ed_by_area = (
            eds_overlayed_onto_postcodes
            >> _get_area_of_each_geometry
            >> _get_postcode_with_largest_geometric_area_overlap_for_each_ed
        )
        """NOTE
            eds_overlayed_onto_postcodes saves result to 
            EDS_OVERLAYED_ONTO_POSTCODES
            on first run SO must delete with delete_intermediate_results
            if either dublin_eds or postcodes parameters change """

        eds_linked_to_postcodes = _link_eds_to_postcodes(
            dublin_eds=dublin_eds, ed_postcode_link=largest_postcode_in_each_ed_by_area
        ) >> _link_failed_matches_to_postcodes_with_sjoin(postcodes)
        """NOTE
                _link_failed_matches_to_postcodes_with_sjoin 
                is a workaround as overlay fails for 2 EDs... """

        _save_geodataframe_result(eds_linked_to_postcodes)
        _annotate_and_plot_result(eds_linked_to_postcodes)

    return flow


# NOTE: run_flow(flow_function=dublin_eds_etl_flow, viz_path=DUBLIN_EDS_FLOW)


def delete_intermediate_results() -> None:
    """ Delete files created during flow if wish to recalculate intermediate
        results
        """

    rmtree(EDS_OVERLAYED_ONTO_POSTCODES)

