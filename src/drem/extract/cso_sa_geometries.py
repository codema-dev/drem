#!/usr/bin/env python

from os import remove
from pathlib import Path
from zipfile import ZipFile

from prefect import task

from drem._filepaths import EXTERNAL_DIR
from drem.extract.download import download


@task(name="Download Small Area Geometries")
def extract_cso_sa_geometries() -> Path:
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

    return unzipped_geometries
