from os import path
from pathlib import Path
from typing import Any
from typing import Optional

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import prefect

from prefect import task


@task
def excel_to_parquet(input_filepath: str, output_filepath: str) -> None:
    """Convert excel file to parquet.

    Args:
        input_filepath (str): Path to input file
        output_filepath (str): Path to output file
    """
    logger = prefect.context.get("logger")

    if path.exists(output_filepath):
        logger.info(f"{output_filepath} already exists")
    else:
        excel = pd.read_excel(input_filepath, engine="openpyxl")
        excel.to_parquet(output_filepath)


@task
def csv_to_parquet(input_filepath: str, output_filepath: str, **kwargs: Any) -> None:
    """Convert csv file to parquet.

    Args:
        input_filepath (str): Path to input file
        output_filepath (str): Path to output file
        **kwargs (Any): Passed to pandas.read_csv
    """
    logger = prefect.context.get("logger")

    if path.exists(output_filepath):
        logger.info(f"{output_filepath} already exists")
    else:
        csv = pd.read_csv(input_filepath, **kwargs)
        csv.to_parquet(output_filepath)


@task
def csv_to_dask_parquet(
    input_filepath: str, output_filepath: str, **kwargs: Any,
) -> None:
    """Convert csv file to parquet.

    Args:
        input_filepath (str): Path to input file
        output_filepath (str): Path to output file
        **kwargs (Any): Passed to pandas.read_csv
    """
    logger = prefect.context.get("logger")

    if path.exists(output_filepath):
        logger.info(f"{output_filepath} already exists")
    else:
        csv = dd.read_csv(input_filepath, **kwargs)
        csv.to_parquet(output_filepath, schema="infer")


@task
def shapefile_to_parquet(
    input_filepath: str, output_filepath: str, path_to_shapefile: Optional[str] = None,
) -> None:
    """Convert ESRI Shapefile to parquet.

    Args:
        input_filepath (str): Path to input file
        output_filepath (str): Path to output file
    """
    logger = prefect.context.get("logger")
    if path.exists(output_filepath):
        logger.info(f"{output_filepath} already exists")
    else:
        shapefile = gpd.read_file(input_filepath, driver="ESRI Shapefile")
        shapefile.to_parquet(output_filepath)
