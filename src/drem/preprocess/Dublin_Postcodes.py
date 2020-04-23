from typing import List, Tuple

import geopandas as gpd  # to read/write spatial data
import matplotlib.pyplot as plt
from pipeop import pipes
from prefect import Flow, task
from prefect.core.task import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from codema_drem.utilities.preprocessing import (
    convert_string_columns_to_categorical,
    infer_column_data_types,
    set_column_strings_to_lowercase,
)

from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
    DUBLIN_POSTCODES_RAW,
    DUBLIN_POSTCODES_CLEAN,
    DUBLIN_POSTCODES_PLOT,
    DUBLIN_POSTCODES_FLOW,
)


@pipes
def postcode_etl_flow() -> Flow:

    with Flow("Preprocess M&R") as flow:

        clean_postcodes = (
            _load_dublin_postcodes
            >> infer_column_data_types
            >> _drop_columns
            >> _rename_columns
            >> set_column_strings_to_lowercase
            >> _set_non_numeric_postcodes_to_dublin_county
            >> _dissolve_co_dublin_postcodes_into_one_shape
        )

        _save_geodataframe_result(clean_postcodes)
        _annotate_and_plot_result(clean_postcodes)

    return flow


# NOTE: run_flow(flow_function=postcode_etl_flow, viz_path=DUBLIN_POSTCODES_FLOW)


# ---------------------------------------------------------------------------


@task
def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    return gpd.read_file(filename=DUBLIN_POSTCODES_RAW, crs="epsg:2157").to_crs(
        "epsg:4326"
    )


@task
def _drop_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.drop(labels=["County", "Yelp_Area"], axis="columns",)


@task
def _rename_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.rename(columns={"Yelp_postc": "postcodes"})


@task
def _set_postcodes_to_lowercase(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf["postcodes"] = gdf["postcodes"].str.lower()

    return gdf


@task
def _set_non_numeric_postcodes_to_dublin_county(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    # find all postcodes not matching pattern 'dublin <number>'
    # https://stackoverflow.com/questions/1687620/regex-match-everything-but-specific-pattern
    pattern = r"^(?!dublin \d+)"
    mask = gdf["postcodes"].str.contains(pat=pattern)
    gdf.loc[mask, "postcodes"] = "co. dublin"

    return gdf


@task
def _dissolve_co_dublin_postcodes_into_one_shape(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """ Merge all co. dublin pcodes into one shape"""
    return gdf.dissolve(by="postcodes").reset_index()


@task
def _save_geodataframe_result(dublin_postcodes: gpd.GeoDataFrame,) -> None:

    dublin_postcodes.to_file(filename=DUBLIN_POSTCODES_CLEAN)


@task
def _annotate_and_plot_result(dublin_postcodes: gpd.GeoDataFrame,) -> None:
    ax = dublin_postcodes.plot(
        column="postcodes", categorical=True, legend=True, figsize=(15, 15),
    )
    properties = {
        "title": "Annotated Dublin Postcodes",
        "xlabel": "Longitude",
        "ylabel": "Latitude",
    }
    ax.set(**properties)

    fig = ax.get_figure()
    fig.savefig(DUBLIN_POSTCODES_PLOT)
    plt.close(fig)
