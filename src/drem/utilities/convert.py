from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import prefect

from prefect import task

from drem.utilities.zip import unzip_file


@task
def excel_to_parquet(input_dirpath: Path, output_dirpath: Path, filename: str) -> None:
    """Convert excel file to parquet.

    Args:
        input_dirpath (Path): Path to input directory
        output_dirpath (Path): Path to save directory
        filename (str): Name of file
    """
    logger = prefect.context.get("logger")

    filepath_excel = input_dirpath / f"{filename}.xlsx"
    filepath_parquet = output_dirpath / f"{filename}.parquet"
    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        pd.read_excel(filepath_excel, engine="openpyxl").to_parquet(filepath_parquet)


@task
def csv_to_parquet(input_dirpath: Path, output_dirpath: Path, filename: str) -> None:
    """Convert csv file to parquet.

    Args:
        input_dirpath (Path): Path to input directory
        output_dirpath (Path): Path to save directory
        filename (str): Name of file
    """
    logger = prefect.context.get("logger")

    filepath_csv = input_dirpath / f"{filename}.csv"
    filepath_parquet = output_dirpath / f"{filename}.parquet"

    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        pd.read_csv(filepath_csv).to_parquet(filepath_parquet)


@task
def shapefile_to_parquet(
    input_dirpath: Path,
    output_dirpath: Path,
    filename: str,
    zipped: Optional[bool] = False,
    path_to_shapefile: Optional[str] = None,
) -> None:
    """Convert ESRI Shapefile to parquet.

    Note:
        Zipped shapefiles are supported by GeoPandas. If the dataset is in a folder in
            the ZIP file, you have to append its name:
            `zipfile = "gadm36_AFG_shp.zip!data"`
            If there are multiple datasets in a folder in the ZIP file, you also have
            to specify the filename:
            `zipfile = "gadm36_AFG_shp.zip!data/gadm36_AFG_1.shp"`

    Args:
        input_dirpath (Path): Path to input directory
        output_dirpath (Path): Path to save directory
        filename (str): Name of file
        zipped (Optional[bool], optional): File extension (such as 'zip'). Defaults to
            False.
        path_to_shapefile (Optional[str], optional): Path to shapefile from within
            zipped folder. Defaults to None.
    """
    logger = prefect.context.get("logger")

    if zipped:
        unzip_file(input_dirpath / f"{filename}.zip")

    if path_to_shapefile:
        filepath_shapefile = input_dirpath / filename / path_to_shapefile
    else:
        filepath_shapefile = input_dirpath / filename

    filepath_parquet = output_dirpath / f"{filename}.parquet"

    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        gpd.read_file(filepath_shapefile, driver="ESRI Shapefile").to_parquet(
            filepath_parquet,
        )
