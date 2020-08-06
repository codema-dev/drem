#!/usr/bin/env python

import geopandas as gpd
from prefect import task

from drem._filepaths import PROCESSED_DIR


@task(name="Load Small Area Geometries")
def load_cso_sa_geometries(gdf: gpd.GeoDataFrame) -> None:
    """Load transformed Dublin Small Area geometry data to local file.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Transformed Dublin Small Area geometry data
    """
    savepath = PROCESSED_DIR / "small_area_geometries"
    gdf.to_file(savepath)
