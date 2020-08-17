#!/usr/bin/env python

from os import remove
from pathlib import Path

import geopandas as gpd

from prefect import task

from drem.extract.download import download
from drem.extract.zip import unzip_file


CWD = Path.cwd()


@task(name="Extract Ireland Small Area Geometries")
def extract_sa_geometries(savedir: Path = CWD) -> gpd.GeoDataFrame:
    """Download CSO 2016 Census Small Area Geometries.

    Args:
        savedir (Path): Save directory for sa_geometries. Defaults to your
        current working directory (i.e. CWD)

    Returns:
        gpd.GeoDataFrame: Small Area Geometries data

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_sa_geometries.run()
    """
    filepath: Path = savedir / "sa_geometries.parquet"

    if not filepath.exists():

        filepath_unzipped: Path = filepath.with_suffix("")
        filepath_zipped: Path = filepath_unzipped.with_suffix(".zip")

        download(
            url="http://data-osi.opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
            filepath=filepath_zipped,
        )

        unzip_file(filepath_zipped)

        gpd.read_file(filepath_unzipped / "sa_geometries").to_parquet(filepath)

        remove(filepath_zipped)

    return gpd.read_parquet(filepath)
