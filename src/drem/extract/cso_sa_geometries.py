#!/usr/bin/env python

from os import remove
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import icontract
import numpy as np

from prefect import task

from drem.extract.download import download
from drem.filepaths import EXTERNAL_DIR


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def _read_only_dublin_local_authorities(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["DÃºn Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


@task(name="Download Small Area Geometries")
def extract_cso_sa_geometries() -> gpd.GeoDataFrame:
    """Download CSO 2016 Census Small Area Geometries.

    Returns:
        Path: filepath to downloaded data
    """
    unzipped_geometries: Path = EXTERNAL_DIR / "cso_sa_geometries"

    if not unzipped_geometries.exists():

        zipped_geometries: Path = EXTERNAL_DIR / "cso_sa_geometries.zip"
        download(
            url="http://data-osi.opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
            filepath=zipped_geometries,
        )

        with ZipFile(zipped_geometries, "r") as zipped_file:
            zipped_file.extractall(unzipped_geometries)

        remove(zipped_geometries)

    return _read_only_dublin_local_authorities(unzipped_geometries)
