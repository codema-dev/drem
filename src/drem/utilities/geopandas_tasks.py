from pathlib import Path
from typing import Any

import geopandas as gpd

from prefect import task


@task
def change_crs(gdf: gpd.GeoDataFrame, **kwargs: Any) -> gpd.GeoDataFrame:
    """Transform geometries to a new coordinate reference system.

    See https://geopandas.org/reference.html?highlight=to_crs#geopandas.GeoDataFrame.to_crs

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame
        **kwargs (Any): Passed to geopandas.GeoDataFrame.to_crs

    Returns:
        gpd.GeoDataFrame: GeoDataFrame
    """
    return gdf.to_crs(**kwargs)


@task
def read_parquet(filepath: Path, **kwargs: Any) -> gpd.GeoDataFrame:
    """Load a Parquet object from the file path, returning a GeoDataFrame.

    See https://geopandas.org/reference/geopandas.read_parquet.html

    Args:
        filepath (Path): Path to file
        **kwargs (Any): Passed to geopandas.read_parquet

    Returns:
        gpd.GeoDataFrame: GeoDataFrame
    """
    return gpd.read_parquet(filepath, **kwargs)


@task
def read_file(filepath: Path, **kwargs: Any) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame from a file or URL.

    See https://geopandas.org/reference/geopandas.read_file.html

    Args:
        filepath (Path): Path to file
        **kwargs (Any): Passed to geopandas.read_file

    Returns:
        gpd.GeoDataFrame: GeoDataFrame
    """
    return gpd.read_file(filepath, **kwargs)
