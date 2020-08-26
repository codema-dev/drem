from os import mkdir
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from icontract import ViolationError
from shapely.geometry import Point

from drem.utilities.parquet_metadata import add_file_engine_metadata_to_parquet_file
from drem.utilities.sample_data import _create_sample_data
from drem.utilities.sample_data import create_test_data


@pytest.fixture
def input_dir(tmp_path) -> Path:
    """Create a temporary input directory containing parquet files with metadata.

    Args:
        tmp_path (Path): Pytest temporary path plugin

    Returns:
        Path: Path to a temporary input directory containing parquet files with metadata
    """
    input_dir = tmp_path / "input"
    mkdir(input_dir)

    pandas_filepath = input_dir / "pandas_file.parquet"
    pd.DataFrame({"col": [1]}).to_parquet(pandas_filepath)
    add_file_engine_metadata_to_parquet_file(pandas_filepath, file_engine="pandas")

    geopandas_filepath = input_dir / "geopandas_file.parquet"
    gpd.GeoDataFrame({"col": [1], "geometry": [Point(1, 1)]}).to_parquet(
        geopandas_filepath,
    )
    add_file_engine_metadata_to_parquet_file(
        geopandas_filepath, file_engine="geopandas",
    )

    return input_dir


@pytest.fixture
def output_dir(tmp_path) -> Path:
    """Create an empty temporary output directory.

    Args:
        tmp_path (Path): Pytest temporary path plugin

    Returns:
        Path: Path to an empty temporary output directory
    """
    output_dir = tmp_path / "output"
    mkdir(output_dir)

    return output_dir


def test_create_sample_data_raises_filenotexists(
    input_dir: Path, output_dir: Path,
) -> None:
    """Raise error if input file doesn't exist.

    Args:
        input_dir (Path): Path to a temporary input directory containing parquet files
        with metadata
        output_dir (Path): Path to an empty temporary output directory
    """
    nonexisting_file = input_dir / "nonexistingfile.parquet"
    output_filepath = output_dir / "nonexistingfile.parquet"
    with pytest.raises(ViolationError):
        _create_sample_data(
            nonexisting_file, output_filepath, file_engine="pandas", sample_size=100,
        )


def test_create_sample_data_raises_not_parquet(
    input_dir: Path, output_dir: Path,
) -> None:
    """Raise error if input file is not parquet.

    Args:
        input_dir (Path): Path to a temporary input directory containing parquet files
        with metadata
        output_dir (Path): Path to an empty temporary output directory
    """
    nonexisting_file = input_dir / "nonexistingfile.csv"
    output_filepath = output_dir / "nonexistingfile.parquet"
    with pytest.raises(ViolationError):
        _create_sample_data(
            nonexisting_file, output_filepath, file_engine="pandas", sample_size=100,
        )


def test_create_sample_data_uses_len_data_if_sample_size_bigger(
    input_dir: Path, output_dir: Path,
) -> None:
    """Function uses row length of 1 instead of sample_size if smaller.

    Args:
        input_dir (Path): Path to a temporary input directory containing parquet files
        with metadata
        output_dir (Path): Path to an empty temporary output directory
    """
    input_filepath = input_dir / "pandas_file.parquet"
    output_filepath = output_dir / "pandas_file.parquet"
    _create_sample_data(
        input_filepath, output_filepath, file_engine="pandas", sample_size=10,
    )

    output = pd.read_parquet(output_filepath)
    assert len(output) == 1


def test_create_sample_data(input_dir: Path, output_dir: Path) -> None:
    """Test sample data created for a single filepath.

    Args:
        input_dir (Path): Path to a temporary input directory containing parquet files
        with metadata
        output_dir (Path): Path to an empty temporary output directory
    """
    input_filepath = input_dir / "pandas_file.parquet"
    output_filepath = output_dir / "pandas_file.parquet"
    _create_sample_data(
        input_filepath, output_filepath, file_engine="pandas", sample_size=1,
    )

    assert output_filepath.exists()


def test_create_test_data_creates_output_files(
    input_dir: Path, output_dir: Path,
) -> None:
    """All input parquet files create output parquet files.

    Args:
        input_dir (Path): Path to a temporary directory containing files with
        file_engine metadata
        output_dir (Path): Path to an empty output_dir
    """
    create_test_data(input_dir, output_dir, sample_size=1)
    output_files = list(output_dir.glob("*.parquet"))

    assert output_files
