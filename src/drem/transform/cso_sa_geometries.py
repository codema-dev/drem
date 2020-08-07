#!/usr/bin/env python

from pathlib import Path

import geopandas as gpd
import icontract
import numpy as np

from prefect import task


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def _read_only_dublin_local_authorities(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["DÃºn Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


def _set_coordinate_reference_system_to_wgs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf.crs = "epsg:4326"

    return gdf


@task(name="Transform Small Area Geometries")
def transform_cso_sa_geometries(geometries: Path) -> gpd.GeoDataFrame:
    """Transform Small Area geometries from raw to clean.

    Extract only Dublin information out of Ireland data
    and a select number of relevant columns.

    Args:
        geometries (Path): Filepath to a locally stored download of the data

    Returns:
        gpd.GeoDataFrame: Small Area Dublin geometries with a select number of columns
    """
    geometries_gdf: gpd.GeoDataFrame = _read_only_dublin_local_authorities(geometries)

    return (
        geometries_gdf.pipe(_set_coordinate_reference_system_to_wgs)
        .loc[:, ["SMALL_AREA", "EDNAME", "geometry"]]
        .rename(columns={"SMALL_AREA": "sas", "EDNAME": "eds"})
        .assign(eds=lambda x: x["eds"].str.lower())
    )
