from pathlib import Path

from prefect import Flow, task
from pipeop import pipes
from toolz.functoolz import pipe
import geopandas as gpd  # to read/write spatial data
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from codema_drem._filepaths import SAS_RAW, SAS_CLEAN
from codema_drem.utilities.flow import run_flow


# Tasks
# *****


@task
def _load_dublin_geodataframe(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["Dún Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


@task
def _set_coordinate_reference_system_to_wgs(gdf: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    gdf.crs = "epsg:4326"

    return gdf


@task
def _extract_columns(dublin_sas: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_sas[["SMALL_AREA", "EDNAME", "geometry"]]


@task
def _rename_columns(dublin_sas: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_sas.rename(columns={"SMALL_AREA": "sas", "EDNAME": "eds"})


@task
def _set_eds_to_lowercase(dublin_sas: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:

    return dublin_sas.assign(eds=dublin_sas["eds"].str.lower())


@task
def _save_geodataframe(gdf: gpd.GeoDataFrame, filepath: Path) -> None:

    gdf.to_file(filepath)


# Flows
# *****


@pipes
def preprocess_small_area_map() -> Flow:

    with Flow("Preprocess Small Area Map") as flow:

        sas_clean = (
            _load_dublin_geodataframe(SAS_RAW)
            >> _set_coordinate_reference_system_to_wgs
            >> _extract_columns
            >> _rename_columns
            >> _set_eds_to_lowercase
        )

        _save_geodataframe(sas_clean, SAS_CLEAN)

    return flow
