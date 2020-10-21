from pathlib import Path

import geopandas as gpd
import pandas as pd

from shapely.geometry import Point

from drem.utilities import convert


def test_convert_csv_to_parquet(tmp_path: Path) -> None:
    """Convert csv file to parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    input_filepath = tmp_path / "data.csv"
    output_filepath = tmp_path / "data.parquet"
    pd.DataFrame({"col": [1, 2, 3]}).to_csv(input_filepath)

    convert.csv_to_parquet.run(
        input_filepath=input_filepath, output_filepath=output_filepath,
    )

    assert output_filepath.exists()


def test_convert_csv_to_dask_parquet(tmp_path: Path) -> None:
    """Convert csv file to dask parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    input_filepath = tmp_path / "data.csv"
    output_filepath = tmp_path / "data.parquet"
    pd.DataFrame({"col": [1, 2, 3]}).to_csv(input_filepath)

    convert.csv_to_dask_parquet.run(
        input_filepath=input_filepath, output_filepath=output_filepath,
    )

    assert output_filepath.exists()


def test_convert_excel_to_parquet(tmp_path: Path) -> None:
    """Convert excel file to parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    input_filepath = tmp_path / "data.xlsx"
    output_filepath = tmp_path / "data.parquet"
    pd.DataFrame({"col": [1, 2, 3]}).to_excel(
        input_filepath, engine="openpyxl",
    )

    convert.excel_to_parquet.run(
        input_filepath=input_filepath, output_filepath=output_filepath,
    )

    assert output_filepath.exists()


def test_convert_shapefile_to_parquet(tmp_path: Path) -> None:
    """Convert shapefile to parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    input_filepath = tmp_path / "data"
    output_filepath = tmp_path / "data.parquet"
    gpd.GeoDataFrame({"data": [1], "geometry": [Point(0, 0)]}).to_file(
        input_filepath, driver="ESRI Shapefile",
    )

    convert.shapefile_to_parquet.run(
        input_filepath=input_filepath, output_filepath=output_filepath,
    )

    assert output_filepath.exists()
