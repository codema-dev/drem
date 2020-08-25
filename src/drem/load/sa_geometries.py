#!/usr/bin/env python

from pathlib import Path

import geopandas as gpd

from prefect import task


@task(name="Load Small Area Geometries to file")
def load_sa_geometries(geometries: gpd.GeoDataFrame, savedir: Path) -> None:
    """Load transformed Dublin Small Area geometry data to local file.

    Args:
        geometries (gpd.GeoDataFrame): Transformed Dublin Small Area geometry data
        savedir (Path): Save directory for data
    """
    geometries.to_parquet(savedir / "sa_geometries.parquet")
