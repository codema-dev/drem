#!/usr/bin/env python

from os import remove
from pathlib import Path

import geopandas as gpd

from prefect import task

from drem.extract.download import download
from drem.extract.zip import unzip_file


CWD = Path.cwd()


@task(name="Download Dublin Postcode Geometries")
def extract_dublin_postcodes(savedir: Path = CWD) -> gpd.GeoDataFrame:
    """Download Dublin Postcode Geometries.

    Args:
        savedir (Path): Save directory for sa_glossary.
        Defaults to your current working directory (i.e. CWD)

    Returns:
        gpd.GeoDataFrame: Dublin Postcode Geometry data

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_dublin_postcodes.run()
    """
    filepath = savedir / "dublin_postcodes.parquet"

    if not filepath.exists():

        filepath_zipped: Path = filepath.with_suffix(".zip")

        download(
            url="https://github.com/rdmolony/dublin-postcode-shapefiles/archive/master.zip",
            filepath=filepath_zipped,
        )

        unzip_file(filepath_zipped)
        remove(filepath_zipped)

        filepath_to_shapefile: Path = (
            savedir
            / "dublin_postcodes"
            / "dublin-postcode-shapefiles-master"
            / "Postcode_dissolve"
        )

        gpd.read_file(filepath_to_shapefile).to_parquet(filepath)

    return gpd.read_parquet(filepath)
