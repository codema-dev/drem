#!/usr/bin/env python

import geopandas as gpd
import icontract
import numpy as np

from prefect import task
from unidecode import unidecode


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def _extract_dublin_local_authorities(
    geometries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return geometries.assign(COUNTYNAME=lambda x: x["COUNTYNAME"].apply(unidecode))[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["Dun Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


def _set_coordinate_reference_system_to_wgs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf.crs = "epsg:4326"

    return gdf


@task(name="Transform Small Area Geometries")
def transform_sa_geometries(geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform Small Area geometries from raw to clean.

    Extract only Dublin information out of Ireland data
    and a select number of relevant columns.

    Args:
        geometries (gpd.GeoDataFrame): Filepath to a locally stored download of the data

    Returns:
        gpd.GeoDataFrame: Small Area Dublin geometries with a select number of columns
    """
    return (
        geometries.pipe(_extract_dublin_local_authorities)
        .pipe(_set_coordinate_reference_system_to_wgs)
        .loc[:, ["SMALL_AREA", "geometry"]]
        .rename(columns={"SMALL_AREA": "small_area"})
    )
