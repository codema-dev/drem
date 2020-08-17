from os import mkdir
from pathlib import Path
from zipfile import ZipFile

from icontract import require


@require(lambda filepath: isinstance(filepath, Path))
@require(lambda filepath: filepath.suffix == ".zip")
def unzip_file(filepath: Path) -> None:
    """Unzip file to an unzipped directory.

    Args:
        filepath (Path): File to unzip
    """
    unzipped_dir: Path = filepath.with_suffix("")
    mkdir(unzipped_dir)
    with ZipFile(filepath, "r") as zipped_file:
        zipped_file.extractall(unzipped_dir)
