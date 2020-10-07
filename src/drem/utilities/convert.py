from pathlib import Path
from typing import Any
from typing import Optional

import geopandas as gpd
import pandas as pd
import prefect

from prefect import task


@task
def excel_to_parquet(dirpath: Path, filename: str) -> None:
    """Convert excel file to parquet.

    Args:
        dirpath (Path): Path to input directory
        filename (str): Name of file
    """
    logger = prefect.context.get("logger")

    filepath_excel = dirpath / f"{filename}.xlsx"
    filepath_parquet = dirpath / f"{filename}.parquet"
    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        pd.read_excel(filepath_excel, engine="openpyxl").to_parquet(filepath_parquet)


@task
def csv_to_parquet(
    dirpath: Path, filename: str, file_extension: str = "csv", **kwargs: Any,
) -> None:
    """Convert csv file to parquet.

    Args:
        dirpath (Path): Path to input directory
        filename (str): Name of file
        file_extension (str): Name of file extension. Defaults to "csv"
        **kwargs (Any): Passed to pandas.read_csv
    """
    logger = prefect.context.get("logger")

    filepath_csv = dirpath / f"{filename}.{file_extension}"
    filepath_parquet = dirpath / f"{filename}.parquet"

    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        pd.read_csv(filepath_csv, **kwargs).to_parquet(filepath_parquet)


@task
def shapefile_to_parquet(
    dirpath: Path, filename: str, path_to_shapefile: Optional[str] = None,
) -> None:
    """Convert ESRI Shapefile to parquet.

    Args:
        dirpath (Path): Path to input directory
        filename (str): Name of file
        path_to_shapefile (Optional[str], optional): Path to shapefile from within
            folder (if '.shp' is nested in an inner-folder). Defaults to None.
    """
    logger = prefect.context.get("logger")

    if path_to_shapefile:
        filepath_shapefile = dirpath / filename / path_to_shapefile
    else:
        filepath_shapefile = dirpath / filename

    filepath_parquet = dirpath / f"{filename}.parquet"

    if filepath_parquet.exists():
        logger.info(f"{filepath_parquet} already exists")
    else:
        gpd.read_file(filepath_shapefile, driver="ESRI Shapefile").to_parquet(
            filepath_parquet,
        )
