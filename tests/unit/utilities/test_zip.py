from os import listdir
from os import mkdir
from pathlib import Path
from shutil import make_archive
from shutil import rmtree
from typing import Generator

import pytest

from _pytest.logging import LogCaptureFixture

from drem.utilities.zip import unzip_directory


@pytest.fixture
def tmp_path_with_zipped_directory(tmp_path: Path) -> Path:
    """Create a zipped directory within tmp_path.

    Args:
        tmp_path (Path): a pytest plugin to create temporary directories

    Returns:
        Path: tmp_path containing a zipped directory
    """
    directory = tmp_path / "directory"
    mkdir(directory)
    data = directory / "data.csv"
    with open(data, mode="a") as file:
        file.write("1,2,3")

    make_archive(directory, "zip", tmp_path)
    rmtree(directory)

    return tmp_path


def test_unzip_file(tmp_path_with_zipped_directory: Path) -> None:
    """Zipped file exists in unzipped folder.

    Args:
        tmp_path_with_zipped_directory (Path): tmp_path containing a zipped directory
    """
    zipped_directory = tmp_path_with_zipped_directory / "directory.zip"
    unzip_directory(zipped_directory)

    assert "data.csv" in listdir(tmp_path_with_zipped_directory / "directory")


def test_unzip_file_skips_if_unzipped_file_exists(
    tmp_path_with_zipped_directory: Path,
    caplog: Generator[LogCaptureFixture, None, None],
) -> None:
    """Skip zipping if unzipped file exists.

    Args:
        tmp_path_with_zipped_directory (Path):  tmp_path containing a zipped directory
        caplog (Generator[LogCaptureFixture, None, None]): See
            https://docs.pytest.org/en/stable/logging.html
    """
    unzipped_directory = tmp_path_with_zipped_directory / "directory"
    mkdir(unzipped_directory)
    with open(unzipped_directory / "data.csv", "w") as file:
        file.write("1,2,3")

    unzip_directory(tmp_path_with_zipped_directory / "directory.zip")

    assert "already" in caplog.text
    assert "unzipped" in caplog.text
