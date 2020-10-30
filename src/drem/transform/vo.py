from pathlib import Path
from re import IGNORECASE
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception


from drem.filepaths import EXTERNAL_DIR
from drem.filepaths import PROCESSED_DIR
from drem.filepaths import DATA_DIR
from drem.transform.benchmarks import transform_benchmarks

from drem.utilities.visualize import VisualizeMixin


@task
def _merge_local_authority_files(dirpath) -> pd.DataFrame:

    files = dirpath.glob("*.csv")
    df = [pd.read_csv(fp) for fp in files]

    return pd.concat(df)


@task
def _fillna_in_columns_where_column_name_contains_substring(
    df: pd.DataFrame, substring: str, replace_with: str,
) -> pd.DataFrame:

    columns = df.filter(regex=substring).columns
    df[columns] = df[columns].fillna(replace_with)

    return df


@task
def _merge_string_columns_into_one(
    df: pd.DataFrame, target: str, result: str,
) -> pd.DataFrame:

    columns = df.filter(regex=target).columns
    df[result] = df[columns].astype(str).agg(" ".join, axis=1)

    return df


@task
def _strip_whitespace(df: pd.DataFrame, target: str, result: str) -> pd.DataFrame:

    df[result] = df[target].astype(str).str.strip()

    return df


@task
def _remove_null_address_strings(df: pd.DataFrame, on: str) -> pd.DataFrame:

    df[on] = (
        df[on].astype(str).str.replace("none|nan", "", flags=IGNORECASE).str.strip()
    )

    return df


@task
def _remove_zero_floor_area_buildings(df: pd.DataFrame) -> pd.DataFrame:

    df = df[df["Area"] > 0]

    return df


@task
def _replace_rows_equal_to_string(
    df: pd.DataFrame, target: str, result: str, to_replace: str, replace_with: str,
) -> pd.DataFrame:

    df[result] = df[target].replace({to_replace: replace_with}, regex=False)

    return df


@task
def _remove_symbols_from_column_strings(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df[column] = df[column].astype(str).str.replace(r"[-,]", "").str.strip()

    return df


@task
def _remove_whitespace_from_column_strings(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = df.columns.str.strip()

    return df


@task
def _extract_use_from_vo_uses_column(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    uses = (
        vo["Uses"]
        .str.split(", ", expand=True)
        .replace("-", np.nan)
        .fillna(np.nan)
        .dropna(axis="columns", how="all")
    )

    use_columns = [f"use_{use_number}" for use_number in uses.columns]
    vo[use_columns] = uses

    return vo


@task
def _merge_benchmarks_into_vo(
    vo: pd.DataFrame, benchmarks: pd.DataFrame,
) -> pd.DataFrame:

    return vo.merge(
        benchmarks, left_on="use_0", right_on="vo_use", how="left", indicator=True,
    )


@task
def _save_unmatched_vo_uses_to_text_file(
    vo: pd.DataFrame, none_file: Path,
) -> pd.DataFrame:

    unmatched_vo_uses = vo.query("`_merge` == 'left_only'")["use_0"].unique().tolist()
    unmatched_vo_uses_with_newlines = [f"{use}\n" for use in unmatched_vo_uses]
    with open(none_file, "w") as file:
        file.writelines(unmatched_vo_uses_with_newlines)

    return vo


@task
def _apply_benchmarks_to_vo_floor_area(vo: pd.DataFrame) -> pd.DataFrame:

    vo["typical_electricity_demand"] = vo["Area"] * vo["typical_electricity"]
    vo["typical_fossil_fuel_demand"] = vo["Area"] * vo["typical_fossil_fuel"]

    return vo


@task
def _convert_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert DataFrame to GeoDataFrame from ITM Coordinates.

    Note:
        - epsg:2157 represents ITM

    Args:
        df (pd.DataFrame): [description]

    Returns:
        gpd.GeoDataFrame: [description]
    """
    coordinates = gpd.points_from_xy(x=df["X ITM"], y=df["Y ITM"])
    return gpd.GeoDataFrame(df, geometry=coordinates, crs="epsg:2157")


@task
def _set_coordinate_reference_system_to_lat_long(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return gdf.to_crs("epsg:4326")


with Flow("Transform Raw VO") as flow:

    """Tidy Valuation Office dataset.

     Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method

    By:
    - Merging all downloaded vo files and creating a single df
    - Clean address names by removing whitespace
    - Remove 0 floor area buildings
    - Fill column names containing substring
    - Merge address columns into a single address column
    - Extract useful colums
    - Clean 'Uses' by removing symbols [-,]
    - Merge combined vo df with benchmarks
    - Save umnatched benchmarks to txt file
    - Apply benchmarks to compute electricity and FF demand
    - Convert into GeoDataFrame so we can perform spatial operations such as plotting
    - Convert to Latitude | Longitude

    """

    data_dir = Parameter("data_dir", default=DATA_DIR)
    external_dir = data_dir / "external"
    benchmarks_dir = data_dir / "commercial_building_benchmarks"

    benchmarks = transform_benchmarks(benchmarks_dir)
    vo_dirpath = external_dir / "vo"

    vo_raw = _merge_local_authority_files(vo_dirpath)
    vo_removed = _remove_whitespace_from_column_strings(vo_raw)
    vo_area = _remove_zero_floor_area_buildings(vo_removed)
    vo_filled = _fillna_in_columns_where_column_name_contains_substring(
        vo_area, substring="Address", replace_with="",
    )
    vo_merged = _merge_string_columns_into_one(
        vo_filled, target="Address", result="address_raw"
    )
    vo_stripped = _strip_whitespace(
        vo_merged, target="address_raw", result="address_stripped"
    )
    vo_replaced = _replace_rows_equal_to_string(
        vo_stripped,
        target="address_stripped",
        result="Address",
        to_replace="",
        replace_with="None",
    )
    vo_extracted = _extract_use_from_vo_uses_column(vo_replaced)
    vo_merged_benchmarks = _merge_benchmarks_into_vo(vo_extracted, benchmarks)
    vo_save_unmatched = _save_unmatched_vo_uses_to_text_file(
        vo_merged_benchmarks, benchmarks_dir / "Unmatched.txt"
    )
    vo_applied = _apply_benchmarks_to_vo_floor_area(vo_save_unmatched)
    vo_gdf = _convert_to_geodataframe(vo_applied)
    vo_crs = _set_coordinate_reference_system_to_lat_long(vo_gdf)


class TransformVO(Task, VisualizeMixin):
    """Clean VO Data in a Prefect flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(
        self,
        input_filepath: Path,
        data_dir: Path,
        benchmarks_dir: Path,
        external_dir: Path,
    ) -> None:
        """Run flow.

        Args:
            input_filepath (Path): Path to input data
            output_filepath (Path): Path to output data
        """
        with raise_on_exception():
            state = self.flow.run(parameters=dict(vo_dirpath=input_filepath))

        result = state.result[vo_crs].result


transform_vo = TransformVO()

