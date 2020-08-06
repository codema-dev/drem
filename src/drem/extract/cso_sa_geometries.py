from pathlib import Path
from zipfile import ZipFile
from prefect import task
from os import remove

from drem.extract.utilities import download
from drem._filepaths import EXTERNAL_DIR


@task(name="Extract Small Area Geometries")
def run() -> Path:

    filepath_to_unzipped = EXTERNAL_DIR / "cso_sa_geometries"

    if not filepath_to_unzipped.exists():

        filepath_to_zipped = EXTERNAL_DIR / "cso_sa_geometries.zip"
        download(
            url="http://data-osi.opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
            filepath=filepath_to_zipped,
        )

        with ZipFile(filepath_to_zipped, "r") as zipped_file:
            zipped_file.extractall(filepath_to_unzipped)

        remove(filepath_to_zipped)

    return filepath_to_unzipped
