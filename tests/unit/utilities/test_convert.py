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
    filename = "data"
    pd.DataFrame({"col": [1, 2, 3]}).to_csv(tmp_path / f"{filename}.csv")
    expected_file_output = tmp_path / "data.parquet"

    convert.csv_to_parquet.run(
        input_dirpath=tmp_path, output_dirpath=tmp_path, filename=filename,
    )

    assert expected_file_output.exists()


def test_convert_excel_to_parquet(tmp_path: Path) -> None:
    """Convert excel file to parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "data"
    pd.DataFrame({"col": [1, 2, 3]}).to_excel(
        tmp_path / f"{filename}.xlsx", engine="openpyxl",
    )
    expected_file_output = tmp_path / "data.xlsx"

    convert.excel_to_parquet.run(
        input_dirpath=tmp_path, output_dirpath=tmp_path, filename=filename,
    )

    assert expected_file_output.exists()


def test_convert_shapefile_to_parquet(tmp_path: Path) -> None:
    """Convert shapefile to parquet.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "data"
    filepath = tmp_path / filename
    gpd.GeoDataFrame({"data": [1], "geometry": [Point(0, 0)]}).to_file(
        filepath, driver="ESRI Shapefile",
    )
    expected_file_output = tmp_path / "data.parquet"

    convert.shapefile_to_parquet.run(
        input_dirpath=tmp_path, output_dirpath=tmp_path, filename=filename,
    )

    assert expected_file_output.exists()
