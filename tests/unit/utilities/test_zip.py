from os import listdir
from os import mkdir
from pathlib import Path
from shutil import make_archive
from shutil import rmtree
from typing import Generator

import geopandas as gpd
import pytest

from _pytest.logging import LogCaptureFixture
from shapely.geometry import Point

from drem.utilities.zip import unzip


@pytest.fixture
def directory_containing_zipped_folder_with_csv(tmp_path: Path) -> Path:
    """Create a zipped directory within tmp_path.

    Args:
        tmp_path (Path): a pytest plugin to create temporary directories

    Returns:
        Path: tmp_path containing a zipped directory
    """
    folder_to_be_zipped = tmp_path / "directory"
    mkdir(folder_to_be_zipped)
    data = folder_to_be_zipped / "data.csv"
    with open(data, mode="a") as file:
        file.write("1,2,3")

    make_archive(folder_to_be_zipped, "zip", root_dir=tmp_path, base_dir="directory")
    rmtree(folder_to_be_zipped)

    return tmp_path


@pytest.fixture
def directory_containing_zipped_folder_with_shapefile(tmp_path: Path) -> Path:
    """Create a zipped directory within tmp_path.

    Args:
        tmp_path (Path): a pytest plugin to create temporary directories

    Returns:
        Path: tmp_path containing a zipped directory
    """
    folder_to_be_zipped = tmp_path / "directory"
    mkdir(folder_to_be_zipped)
    data = folder_to_be_zipped / "shapefile"
    gpd.GeoDataFrame({"data": [1], "geometry": [Point(0, 0)]}).to_file(data)

    make_archive(folder_to_be_zipped, "zip", tmp_path)
    rmtree(folder_to_be_zipped)

    return tmp_path


@pytest.mark.xfail
def test_unzipped_folder_contains_csv(
    directory_containing_zipped_folder_with_csv: Path,
) -> None:
    """Zipped file exists in unzipped folder.

    Args:
        directory_containing_zipped_folder_with_csv (Path): tmp_path containing a zipped
            directory
    """
    zipped_folder = directory_containing_zipped_folder_with_csv / "directory.zip"
    unzip.run(zipped_folder)

    assert "data.csv" in listdir(
        directory_containing_zipped_folder_with_csv / "directory",
    )


@pytest.mark.xfail
def test_unzipped_folder_contains_shapefile(
    directory_containing_zipped_folder_with_shapefile: Path,
) -> None:
    """Zipped file exists in unzipped folder.

    Args:
        directory_containing_zipped_folder_with_shapefile (Path): tmp_path containing a
            zipped directory
    """
    zipped_folder = directory_containing_zipped_folder_with_shapefile / "directory.zip"
    unzip.run(zipped_folder)

    assert "shapefile" in listdir(
        directory_containing_zipped_folder_with_shapefile / "directory",
    )


def test_unzip_file_skips_if_unzipped_file_exists(
    directory_containing_zipped_folder_with_csv: Path,
    caplog: Generator[LogCaptureFixture, None, None],
) -> None:
    """Skip zipping if unzipped file exists.

    Args:
        directory_containing_zipped_folder_with_csv (Path):  tmp_path containing a
            zipped directory
        caplog (Generator[LogCaptureFixture, None, None]): See
            https://docs.pytest.org/en/stable/logging.html
    """
    unzipped_directory = directory_containing_zipped_folder_with_csv / "directory"
    mkdir(unzipped_directory)
    with open(unzipped_directory / "data.csv", "w") as file:
        file.write("1,2,3")

    unzip.run(dirpath=directory_containing_zipped_folder_with_csv, filename="directory")

    assert "already" in caplog.text
    assert "unzipped" in caplog.text
