from pathlib import Path

import geopandas as gpd
from prefect import task

from drem._filepaths import PROCESSED_DIR


@task(name="Load Small Area Geometries")
def run(gdf: gpd.GeoDataFrame) -> None:

    savepath = PROCESSED_DIR / "small_area_geometries"
    gdf.to_file(savepath)
