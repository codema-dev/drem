#!/usr/bin/env python

from os import remove
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import geopandas as gpd

from prefect import task

from drem.extract.download import download


@task(name="Extract Ireland Small Area Geometries")
def extract_sa_geometries(
    unzipped_geometries: Optional[Path] = None,
) -> gpd.GeoDataFrame:
    """Download CSO 2016 Census Small Area Geometries.

    Args:
        unzipped_geometries (Optional[Path], optional): Save destination for geometries.
        Defaults to None which results in the Geometries being saved to your current
        working directory.

    Returns:
        gpd.GeoDataFrame: Small Area Geometries

    """
    if unzipped_geometries is None:
        unzipped_geometries: Path = Path.cwd() / "sa_geometries"

    if not unzipped_geometries.exists():

        zipped_geometries: Path = unzipped_geometries.with_suffix(".zip")
        download(
            url="http://data-osi.opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
            filepath=zipped_geometries,
        )

        with ZipFile(zipped_geometries, "r") as zipped_file:
            zipped_file.extractall(unzipped_geometries)

        remove(zipped_geometries)

    return gpd.read_file(unzipped_geometries)
