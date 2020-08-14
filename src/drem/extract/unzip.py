from pathlib import Path
from zipfile import ZipFile


def unzip_file(filepath: Path) -> None:
    """Unzip file.

    Args:
        filepath (Path): File to unzip
    """
    with ZipFile(filepath, "r") as zipped_file:
        zipped_file.extractall(filepath.parent)
