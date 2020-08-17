from os import listdir
from pathlib import Path
from shutil import copyfile

from drem.extract.zip import unzip_file
from drem.filepaths import UTEST_DATA_EXTERNAL


ZIPPED_FOLDER = UTEST_DATA_EXTERNAL / "zipped_folder.zip"


def test_unzip_file(tmp_path: Path) -> None:
    """Zipped file exists in unzipped folder.

    Args:
        tmp_path (Path): a pytest plugin to create temporary directories
    """
    temp_zipped_file = tmp_path / "zipped_folder.zip"
    copyfile(ZIPPED_FOLDER, temp_zipped_file)
    unzip_file(temp_zipped_file)

    assert "zipped_file.txt" in listdir(tmp_path / "zipped_folder")
