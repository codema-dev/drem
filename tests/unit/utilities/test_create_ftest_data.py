from os import mkdir
from pathlib import Path
from typing import Set

import geopandas as gpd
import pandas as pd
import pytest

from shapely.geometry import Point

from drem.utilities.ftest_data import create_ftest_data


@pytest.fixture
def input_dir(tmp_path) -> Path:
    """Create a temporary input directory containing parquet files with metadata.

    Args:
        tmp_path (Path): Pytest temporary path plugin

    Returns:
        Path: Path to a temporary input directory containing parquet files with metadata
    """
    input_dir: Path = tmp_path / "input"
    mkdir(input_dir)

    large_sample_size = range(250)
    small_sample_size = range(30)

    pd.DataFrame({"col": list(large_sample_size)}).to_parquet(
        input_dir / "BERPublicsearch.parquet",
    )
    pd.DataFrame({"col": list(large_sample_size)}).to_parquet(
        input_dir / "sa_statistics.parquet",
    )
    pd.DataFrame({"col": list(small_sample_size)}).to_parquet(
        input_dir / "sa_glossary.parquet",
    )
    gpd.GeoDataFrame(
        {
            "col": list(large_sample_size),
            "geometry": [
                Point(x, y) for x, y in zip(large_sample_size, large_sample_size)
            ],
        },
    ).to_parquet(input_dir / "sa_geometries.parquet")
    gpd.GeoDataFrame(
        {
            "col": list(small_sample_size),
            "geometry": [
                Point(x, y) for x, y in zip(small_sample_size, small_sample_size)
            ],
        },
    ).to_parquet(input_dir / "dublin_postcodes.parquet")

    return input_dir


@pytest.fixture
def output_dir(tmp_path) -> Path:
    """Create an empty temporary output directory.

    Args:
        tmp_path (Path): Pytest temporary path plugin

    Returns:
        Path: Path to an empty temporary output directory
    """
    output_dir: Path = tmp_path / "output"
    mkdir(output_dir)

    return output_dir


@pytest.fixture
def output_dir_with_ftest_data(input_dir: Path, output_dir: Path) -> Path:
    """Create output directory with functional test data.

    Args:
        input_dir (Path): Path to directory containing input data
        output_dir (Path): Path to directory where output data will be saved

    Returns:
        Path: Path to directory where output data has been saved
    """
    create_ftest_data(input_dir, output_dir)
    return output_dir


def test_sample_extract_folder_data_contains_file(
    output_dir_with_ftest_data: Path,
) -> None:
    """Output directory contains all of the expected files.

    Args:
        output_dir_with_ftest_data (Path): Path to directory where output data has been
        saved
    """
    expected_filenames: Set[str] = {
        "BERPublicsearch.parquet",
        "sa_statistics.parquet",
        "sa_glossary.parquet",
        "sa_geometries.parquet",
        "dublin_postcodes.parquet",
    }

    output_dir_filenames: Set[str] = {
        file.name for file in output_dir_with_ftest_data.glob("*.parquet")
    }

    assert expected_filenames == output_dir_filenames
