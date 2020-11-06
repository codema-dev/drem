from pathlib import Path
from os import path
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
from drem.utilities.filepaths import EXTERNAL
from drem.utilities.breakpoint import flow_breakpoint


@task
def _merge_local_authority_files(dirpath: Path) -> pd.DataFrame:

    dirpath_path = Path(dirpath)
    files = dirpath_path.glob("*.csv")
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

    return df.copy().loc[df["Area"] > 0]


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

    benchmarks_dir = EXTERNAL["commercial_benchmarks"]
    benchmarks = transform_benchmarks(benchmarks_dir)
    vo_dirpath = EXTERNAL["vo"]

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
    unmatched_file = path.join(benchmarks_dir, "Unmatched.txt")
    vo_save_unmatched = _save_unmatched_vo_uses_to_text_file(
        vo_merged_benchmarks, unmatched_file
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

    def run(self) -> gpd.GeoDataFrame:
        """Run flow.

        Args:
            input_filepath (Path): Path to input data

        Returns:
            Clean GeoDataFrame
        """
        with raise_on_exception():
            state = self.flow.run()


transform_vo = TransformVO()

