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
@require(lambda dirpath: isinstance(dirpath, Path))
def unzip(dirpath: Path, filename: str, file_extension: str = "zip") -> None:
    """Unzip directory in Prefect Task.

    Args:
        dirpath (Path): Path to directory
        filename (str): Name of file
        file_extension (str): File extension of file to be unzipped. Defaults to 'zip'
    """
    filepath = dirpath / f"{filename}.{file_extension}"
    filepath_unzipped = dirpath / filename

    if filepath_unzipped.exists():
        logger.info(f"{filepath_unzipped} has already been unzipped!")
    else:
        mkdir(filepath_unzipped)
        unpack_archive(filepath, filepath_unzipped)
