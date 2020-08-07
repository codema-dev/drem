#!/usr/bin/env python

import geopandas as gpd

from prefect import task


def _set_coordinate_reference_system_to_wgs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf.crs = "epsg:4326"

    return gdf


@task(name="Transform Small Area Geometries")
def transform_cso_sa_geometries(geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform Small Area geometries from raw to clean.

    Extract only Dublin information out of Ireland data
    and a select number of relevant columns.

    Args:
        geometries (gpd.GeoDataFrame): Filepath to a locally stored download of the data

    Returns:
        gpd.GeoDataFrame: Small Area Dublin geometries with a select number of columns
    """
    return (
        geometries.pipe(_set_coordinate_reference_system_to_wgs)
        .loc[:, ["SMALL_AREA", "EDNAME", "geometry"]]
        .rename(columns={"SMALL_AREA": "sas", "EDNAME": "eds"})
        .assign(eds=lambda x: x["eds"].str.lower())
    )
