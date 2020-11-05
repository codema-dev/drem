# Create sample data for functional tests.

from os import mkdir
from pathlib import Path
from shutil import copyfile
from shutil import copytree
from shutil import make_archive
from shutil import rmtree

import geopandas as gpd
import pandas as pd

from loguru import logger
from unidecode import unidecode


def _copy(filename: str, input_dirpath: Path, output_dirpath: Path) -> None:

    input_filepath = input_dirpath / filename
    output_filepath = output_dirpath / filename
    if output_filepath.exists():
        logger.info(f"{output_filepath} already exists!")
    else:
        logger.info(f"Copying {input_filepath} to {output_dirpath}")
        copyfile(input_filepath, output_filepath)


def _copy_dublin_sa_geometries(
    filename: str, input_dirpath: Path, output_dirpath: Path,
) -> None:

    input_filepath = input_dirpath / filename
    output_filepath = output_dirpath / filename
    output_filepath_zipped = output_dirpath / f"{filename}.zip"

    if output_filepath_zipped.exists():
        logger.info(f"{output_filepath_zipped} already exists!")
    else:
        logger.info(f"Copying {input_filepath} to {output_filepath}")
        geometries = gpd.read_file(input_filepath)
        geometries.loc[:, "COUNTYNAME"] = geometries["COUNTYNAME"].apply(unidecode)
        mask = geometries["COUNTYNAME"].isin(
            ["Dun Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
        geometries[mask].to_file(output_filepath, index=False)

        logger.info(f"Zipping {output_filepath}")
        make_archive(output_filepath, "zip", output_filepath)
        rmtree(output_filepath)


def _sample_sa_statistics(
    filename: str, input_dirpath: Path, output_dirpath: Path, sample_size: int,
) -> None:

    input_filepath = input_dirpath / filename
    output_filepath = output_dirpath / filename

    if output_filepath.exists():
        logger.info(f"{output_filepath} already exists!")
    else:
        logger.info(f"Copying {input_filepath} to {output_dirpath}")
        sa_stats_sample = pd.read_csv(input_filepath).sample(sample_size)
        sa_stats_sample.to_csv(output_filepath, index=False)


def _copy_dublin_postcodes(
    filename: str, input_dirpath: Path, output_dirpath: Path,
) -> None:

    input_filepath = input_dirpath / filename
    output_filepath = output_dirpath / filename
    output_filepath_zipped = output_dirpath / f"{filename}.zip"

    if output_filepath_zipped.exists():
        logger.info(f"{output_filepath_zipped} already exists!")
    else:
        logger.info(f"Copying {input_filepath} to {output_filepath}")
        copytree(input_filepath, output_filepath)

        logger.info(f"Zipping {output_filepath}")
        make_archive(output_filepath, "zip", output_filepath)
        rmtree(output_filepath)


def _sample_berpublicsearch(
    filename: str,
    input_dirpath: Path,
    output_dirpath: Path,
    sample_size: int,
    path_to_txt: str,
) -> None:

    input_filepath = input_dirpath / filename / path_to_txt
    output_filepath = output_dirpath / filename / path_to_txt
    output_filepath_zipped = output_dirpath / f"{filename}.zip"

    if output_filepath_zipped.exists():
        logger.info(f"{output_filepath_zipped} already exists!")
    else:
        logger.info(f"Creating {output_filepath.parent}")
        mkdir(output_filepath.parent)

        logger.info(f"Copying {input_filepath} to {output_filepath}")
        ber_sample = pd.read_csv(
            input_filepath,
            sep="\t",
            encoding="latin-1",
            error_bad_lines=False,
            low_memory=False,
        ).sample(sample_size)
        ber_sample.to_csv(
            output_filepath, index=False, sep="\t", encoding="latin-1",
        )

        logger.info(f"Zipping {output_filepath.parent}")
        make_archive(output_filepath.parent, "zip", output_filepath.parent)
        rmtree(output_filepath.parent)


def _sample_valuation_office(
    input_dirpath: Path, output_dirpath: Path, sample_size: int,
) -> None:

    if output_dirpath.exists():
        rmtree(output_dirpath)

    mkdir(output_dirpath)

    for filepath in input_dirpath.glob("*.csv"):

        data = pd.read_csv(filepath)
        number_of_samples = len(data)

        if sample_size > number_of_samples:
            sample = data.sample(number_of_samples)
        else:
            sample = data.sample(sample_size)

        sample.to_csv(output_dirpath / filepath.name)


def create_ftest_data(input_dirpath: Path, output_dirpath: Path) -> None:
    """Create sample data for functional testing.

    Args:
        input_dirpath (Path): Path to directory containing input data
        output_dirpath (Path): Path to directory where output data will be saved
    """
    _copy(
        "small_area_glossary_2016.xlsx", input_dirpath, output_dirpath,
    )
    _copy_dublin_sa_geometries(
        "small_area_geometries_2016", input_dirpath, output_dirpath,
    )
    _sample_sa_statistics(
        "small_area_statistics_2016.csv",
        input_dirpath,
        output_dirpath,
        sample_size=200,
    )
    _copy_dublin_postcodes(
        "dublin_postcodes", input_dirpath, output_dirpath,
    )
    _sample_berpublicsearch(
        "BERPublicsearch",
        input_dirpath,
        output_dirpath,
        sample_size=200,
        path_to_txt="BERPublicsearch.txt",
    )
    _sample_valuation_office(
        input_dirpath / "vo", output_dirpath / "external" / "vo", sample_size=20,
    )
