from os import path
from os import mkdir
from pathlib import Path
from shutil import unpack_archive
from zipfile import ZipFile

from icontract import require
from loguru import logger
from prefect import task


@require(lambda filepath: isinstance(filepath, Path))
@require(lambda filepath: filepath.suffix == ".zip")
def unzip_file(filepath: Path) -> None:
    """Unzip file to an unzipped directory.

    Args:
        filepath (Path): File to unzip
    """
    unzipped_dir: Path = filepath.with_suffix("")
    if not unzipped_dir.exists():
        mkdir(unzipped_dir)
        with ZipFile(filepath, "r") as zipped_file:
            zipped_file.extractall(unzipped_dir)


def unzip_folder(filepath: Path) -> None:
    """Unzip a zipped folder.

    Args:
        filepath (Path): Folder to unzip
    """
    dirpath_unzipped: Path = filepath.with_suffix("")

    if dirpath_unzipped.exists():
        logger.info(f"{filepath} has already been unzipped!")
    else:
        mkdir(dirpath_unzipped)
        unpack_archive(filepath, dirpath_unzipped)


@task
def unzip(input_filepath: str, output_filepath: str) -> None:
    """Unzip directory in Prefect Task.

    Args:
        input_filepath (str): Path to file to be zipped
        output_filepath (str): Path to unzipped file
    """
    if path.exists(output_filepath):
        logger.info(f"Skipping as {output_filepath} has already been unzipped!")
    else:
        mkdir(output_filepath)
        unpack_archive(input_filepath, output_filepath)
