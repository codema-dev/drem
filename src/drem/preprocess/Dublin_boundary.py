import geopandas as gpd
from pipeop import pipes
from prefect import Flow, task
from prefect.engine.executors import LocalDaskExecutor
from prefect.utilities.debug import raise_on_exception

from codema_drem._filepaths import (
    DATA_DIR,
    DUBLIN_BOUNDARY,
    DUBLIN_BOUNDARY_FLOW,
    DUBLIN_LAS_RAW,
    PLOT_DIR,
    BASE_DIR,
)


@pipes
def _dublin_boundary_etl_flow():

    with Flow("Extract Dublin boundary from a map of Ireland LAs") as flow:

        dublin_boundary = (
            _load_ireland_admin_areas() >> _dissolve_dublin_geometries_into_one
        )

        _save_result(dublin_boundary)

    return flow


# NOTE: run_flow(flow_function=_dublin_boundary_etl_flow, viz_path=DUBLIN_BOUNDARY_FLOW)


# --------------------------------------------------------------------------


@task
def _load_ireland_admin_areas() -> gpd.GeoDataFrame:

    return gpd.read_file(DUBLIN_LAS_RAW)[["COUNTY", "geometry"]]


@task
def _dissolve_dublin_geometries_into_one(
    ireland_las: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return ireland_las[ireland_las["COUNTY"] == "DUBLIN"].dissolve(by="COUNTY")


@task
def _save_result(dublin_boundary: gpd.GeoDataFrame) -> None:

    dublin_boundary.to_file(DUBLIN_BOUNDARY)
