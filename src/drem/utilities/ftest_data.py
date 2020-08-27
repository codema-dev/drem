#!/usr/bin/env python

# Create sample data for functional tests.

from pathlib import Path
from shutil import copyfile

import geopandas as gpd
import pandas as pd

from drem.transform.sa_geometries import extract_dublin_local_authorities


def _copy_sa_glossary(input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / "sa_glossary.parquet"
    output_filepath = output_dirpath / "sa_glossary.parquet"

    if not output_filepath.exists():
        copyfile(input_filepath, output_filepath)


def _copy_dublin_sa_geometries(input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / "sa_geometries.parquet"
    output_filepath = output_dirpath / "sa_geometries.parquet"

    if not output_filepath.exists():
        gpd.read_parquet(input_filepath).pipe(
            extract_dublin_local_authorities,
        ).to_parquet(output_filepath)


def _sample_sa_statistics(input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / "sa_statistics.parquet"
    output_filepath = output_dirpath / "sa_statistics.parquet"

    if not output_filepath.exists():
        pd.read_parquet(input_filepath).sample(200).to_parquet(output_filepath)


def _copy_dublin_postcodes(input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / "dublin_postcodes.parquet"
    output_filepath = output_dirpath / "dublin_postcodes.parquet"

    if not output_filepath.exists():
        copyfile(input_filepath, output_filepath)


def _sample_berpublicsearch(input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / "BERPublicsearch.parquet"
    output_filepath = output_dirpath / "BERPublicsearch.parquet"

    if not output_filepath.exists():
        pd.read_parquet(input_filepath).sample(200).to_parquet(output_filepath)


def create_ftest_data(input_dirpath: Path, output_dirpath: Path) -> None:
    """Create sample data for functional testing.

    Args:
        input_dirpath (Path): Path to directory containing input data
        output_dirpath (Path): Path to directory where output data will be saved
    """
    _copy_sa_glossary(input_dirpath, output_dirpath)
    _copy_dublin_sa_geometries(input_dirpath, output_dirpath)
    _sample_sa_statistics(input_dirpath, output_dirpath)
    _copy_dublin_postcodes(input_dirpath, output_dirpath)
    _sample_berpublicsearch(input_dirpath, output_dirpath)
