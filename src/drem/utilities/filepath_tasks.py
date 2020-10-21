from functools import wraps
from os import path
from pathlib import Path

from prefect import task


@task(name="Get Filepath")
def get_filepath(data_dir: str, filename: str, file_extension: str) -> str:

    return path.join(data_dir, f"{filename}{file_extension}")


def get_data_dir() -> str:

    cwd = Path(__file__)
    base_dir = cwd.resolve().parents[2]
    data_dir = base_dir / "data"

    return str(data_dir)
