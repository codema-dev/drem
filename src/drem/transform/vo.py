from pathlib import Path
from re import IGNORECASE

import geopandas as gpd
import numpy as np
import pandas as pd

from prefect import Flow
from prefect import task
from drem.filepaths import EXTERNAL_DIR
from drem.filepaths import PROCESSED_DIR

import drem.utilities.dask_dataframe_tasks as ddt
import drem.utilities.pandas_tasks as pdt


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


@task
def transform_vo(
    vo_dirpath: Path, benchmarks: pd.DataFrame, unmatched_txtfile: Path,
) -> gpd.GeoDataFrame:
    """Tidy Valuation Office dataset.

    By:
    - Clean address names by removing whitespace
    - Combine address columns into a single address column
    - Pull out columns: Address, Floor Area, Lat, Long
    - Set columns to titlecase
    - Clean 'Uses' by removing symbols [-,]
    - Remove 0 floor area buildings
    - Convert into GeoDataFrame so we can perform spatial operations such as plotting
    - Convert to Latitude | Longitude

    Args:
        vo_raw (pd.DataFrame): Raw VO DataFrame
        benchmarks (pd.DataFrame): Benchmarks linking VO 'use' to a benchmark energy
        unmatched_txtfile (Path): Path to unmatched vo 'use' categories

    Returns:
        pd.DataFrame: Tidy DataFrame
    """
    return (
        _merge_local_authority_files(vo_dirpath)
        .rename(columns=str.strip)
        .pipe(
            _fillna_in_columns_where_column_name_contains_substring,
            substring="Address",
            replace_with="",
        )
        .pipe(_merge_string_columns_into_one, target="Address", result="address_raw")
        .pipe(_strip_whitespace, target="address_raw", result="address_stripped")
        .pipe(
            _replace_rows_equal_to_string,
            target="address_stripped",
            result="Address",
            to_replace="",
            replace_with="None",
        )
        .pipe(_extract_use_from_vo_uses_column)
        .pipe(_merge_benchmarks_into_vo, benchmarks)
        .pipe(_save_unmatched_vo_uses_to_text_file, unmatched_txtfile)
        .pipe(_apply_benchmarks_to_vo_floor_area)
        .query("Area > 0")
        .pipe(_convert_to_geodataframe)
        .pipe(_set_coordinate_reference_system_to_lat_long)
    )


with Flow("Transform Raw VO") as flow:

    vo_raw = _merge_local_authority_files(dirpath=EXTERNAL_DIR / "vo")
    vo_filled = _fillna_in_columns_where_column_name_contains_substring(
        vo_raw, substring="Address", replace_with="",
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

