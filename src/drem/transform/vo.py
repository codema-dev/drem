from pathlib import Path
from re import IGNORECASE

import geopandas as gpd
import numpy as np
import pandas as pd

from prefect import task


def _merge_address_columns_into_one(df: pd.DataFrame, on: str) -> pd.DataFrame:

    address_columns = df.filter(regex=on).columns
    df[on] = df[address_columns].astype(str).agg(" ".join, axis=1)

    return df


def _remove_null_address_strings(df: pd.DataFrame, on: str) -> pd.DataFrame:

    df[on] = (
        df[on].astype(str).str.replace("none|nan", "", flags=IGNORECASE).str.strip()
    )

    return df


def _replace_string_in_column(
    df: pd.DataFrame, on: str, to_replace: str, replace_with: str,
) -> pd.DataFrame:

    df[on] = df[on].astype(str).str.replace(to_replace, replace_with)

    return df


def _remove_symbols_from_column_strings(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df[column] = df[column].astype(str).str.replace(r"[-,]", "").str.strip()

    return df


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


def _merge_benchmarks_into_vo(
    vo: pd.DataFrame, benchmarks: pd.DataFrame,
) -> pd.DataFrame:

    return vo.merge(
        benchmarks, left_on="use_0", right_on="vo_use", how="left", indicator=True,
    )


def _save_unmatched_vo_uses_to_text_file(
    vo: pd.DataFrame, none_file: Path,
) -> pd.DataFrame:

    unmatched_vo_uses = vo.query("`_merge` == 'left_only'")["use_0"].unique().tolist()
    unmatched_vo_uses_with_newlines = [f"{use}\n" for use in unmatched_vo_uses]
    with open(none_file, "w") as file:
        file.writelines(unmatched_vo_uses_with_newlines)

    return vo


def _apply_benchmarks_to_vo_floor_area(vo: pd.DataFrame) -> pd.DataFrame:

    vo["typical_electricity_demand"] = vo["Area"] * vo["typical_electricity"]
    vo["typical_fossil_fuel_demand"] = vo["Area"] * vo["typical_fossil_fuel"]

    return vo


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


def _set_coordinate_reference_system_to_lat_long(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return gdf.to_crs("epsg:4326")


@task
def transform_vo(
    vo_raw: pd.DataFrame, benchmarks: pd.DataFrame, unmatched_txtfile: Path,
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
        vo_raw.copy()
        .rename(columns=str.strip)
        .pipe(_merge_address_columns_into_one, on="Address")
        .pipe(_remove_null_address_strings, on="Address")
        .pipe(
            _replace_string_in_column, on="Address", to_replace="", replace_with="None",
        )
        .pipe(_extract_use_from_vo_uses_column)
        .pipe(_merge_benchmarks_into_vo, benchmarks)
        .pipe(_save_unmatched_vo_uses_to_text_file, unmatched_txtfile)
        .pipe(_apply_benchmarks_to_vo_floor_area)
        .query("Area > 0")
        .pipe(_convert_to_geodataframe)
        .pipe(_set_coordinate_reference_system_to_lat_long)
    )
