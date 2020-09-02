from typing import List

import geopandas as gpd
import pandas as pd

from prefect import task


def _merge_address_columns_into_one(df: pd.DataFrame) -> pd.DataFrame:

    address_columns = df.filter(regex="Address").columns
    df["Address"] = df[address_columns].astype(str).agg(" ".join, axis=1)

    return df


def _set_column_strings_to_titlecase(
    df: pd.DataFrame, columns: List[str],
) -> pd.DataFrame:
    """Set string in each row to titlecase.

    Args:
        df (pd.DataFrame): [description]
        columns (List[str]): [description]

    Returns:
        pd.DataFrame: [description]

    Example:
        NON-LIST becomes Non-List
    """
    for column in columns:
        df[column] = df[column].astype(str).str.title()

    return df


def _remove_symbols_from_column_strings(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df[column] = df[column].astype(str).str.replace(r"[-,]", "").str.strip()

    return df


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
def transform_vo(vo_raw: pd.DataFrame) -> gpd.GeoDataFrame:
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

    Returns:
        pd.DataFrame: Tidy DataFrame
    """
    return (
        vo_raw.copy()
        .rename(columns=str.strip)
        .pipe(_merge_address_columns_into_one)
        .loc[
            :,
            [
                "Address",
                "Category",
                "Uses",
                "Area",
                "X ITM",
                "Y ITM",
                "Property Number",
                "Valuation",
                "Level",
                "Car Park",
            ],
        ]
        .pipe(_set_column_strings_to_titlecase, ["Category", "Uses"])
        .pipe(_remove_symbols_from_column_strings, "Uses")
        .query("Area > 0")
        .pipe(_convert_to_geodataframe)
        .pipe(_set_coordinate_reference_system_to_lat_long)
    )
