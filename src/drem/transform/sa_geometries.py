#!/usr/bin/env python

import geopandas as gpd
import icontract
import numpy as np

from prefect import task
from unidecode import unidecode


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def extract_dublin_local_authorities(geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Extract Dublin Local Authorities from Ireland Small Area Geometries Data.

    Args:
        geometries (gpd.GeoDataFrame): Ireland Small Area Geometries

    Returns:
        gpd.GeoDataFrame: Dublin Small Area Geometries
    """
    return geometries.assign(COUNTYNAME=lambda x: x["COUNTYNAME"].apply(unidecode))[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["Dun Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


@task(name="Transform Small Area Geometries")
def transform_sa_geometries(geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform Small Area geometries.

    By:
        - Extract Dublin information out of Ireland data.
        - Convert coordinate reference system to latitude | longitude.
        - Select relevant columns.
        - Rename columns to standardised.

    Args:
        geometries (gpd.GeoDataFrame): Filepath to a locally stored download of the data

    Returns:
        gpd.GeoDataFrame: Small Area Dublin geometries with a select number of columns
    """
    return (
        geometries.pipe(extract_dublin_local_authorities)
        .to_crs("epsg:4326")
        .loc[:, ["SMALL_AREA", "geometry"]]
        .rename(columns={"SMALL_AREA": "small_area"})
    )
