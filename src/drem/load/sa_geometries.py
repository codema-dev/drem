#!/usr/bin/env python

import geopandas as gpd

from prefect import task

from drem.filepaths import PROCESSED_DIR


@task(name="Load Small Area Geometries to file")
def load_sa_geometries(geometries: gpd.GeoDataFrame) -> None:
    """Load transformed Dublin Small Area geometry data to local file.

    Args:
        geometries (gpd.GeoDataFrame): Transformed Dublin Small Area geometry data
    """
    savepath = PROCESSED_DIR / "small_area_geometries.parquet"
    geometries.to_parquet(savepath)
