#!/usr/bin/env python

from pathlib import Path
from shutil import rmtree

import geopandas as gpd

from prefect import task

from drem.extract.download import download
from drem.extract.zip import unzip_file
from drem.utilities.parquet_metadata import add_file_engine_metadata_to_parquet_file


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
    filepath_zipped = savedir / "dublin_postcodes.zip"
    filepath_unzipped: Path = savedir / "dublin_postcodes"
    filepath_parquet: Path = savedir / "dublin_postcodes.parquet"

    if not filepath_parquet.exists():

        download(
            url="https://github.com/rdmolony/dublin-postcode-shapefiles/archive/master.zip",
            filepath=filepath_zipped,
        )

        unzip_file(filepath_zipped)

        gpd.read_file(
            filepath_unzipped
            / "dublin-postcode-shapefiles-master"
            / "Postcode_dissolve",
        ).to_parquet(filepath_parquet)

        rmtree(filepath_unzipped)

        add_file_engine_metadata_to_parquet_file(filepath_parquet, "geopandas")

    return gpd.read_parquet(filepath_parquet)
