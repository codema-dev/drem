"""
This script will create a synthetic residential building stock.

Our methodology is to divide each SA into several building types,
using the Split-Apply-Combine pandas transform. These ratios will
then be applied to outputs from EnergyPlus to generate a first-pass
estimate for residential energy demand in Dublin.

"""
import geopandas as gpd
import pandas as pd

from prefect import Flow
from prefect import task

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR


@task
def _read_sa_parquet(input_filepath: str) -> pd.DataFrame:

    return pd.read_parquet(input_filepath).drop_duplicates()


@task
def _read_sa_geometries(input_filepath: str) -> gpd.GeoDataFrame:

    return gpd.read_parquet(input_filepath)


@task
def _read_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _merge_ber_sa(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _split_ber_construction(df: pd.DataFrame, condition: str) -> pd.DataFrame:

    return df.query(condition).drop_duplicates()


@task
def _extract_res(df: pd.DataFrame, on: str, value: str) -> pd.DataFrame:

    return df.loc[df[on] == value].drop_duplicates().reset_index(drop=True)


@task
def _transform_res(df: pd.DataFrame, lon: str, lat: str) -> gpd.GeoDataFrame:

    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat))


@task
def _spatial_join(
    left: gpd.GeoDataFrame, right: gpd.GeoDataFrame, **kwargs,
) -> gpd.GeoDataFrame:

    right = right.set_crs(epsg="4326")

    return gpd.sjoin(left, right)


@task
def _assign_building_type(df: pd.DataFrame, on: str, equiv: list) -> pd.DataFrame:

    return df.replace({on: equiv})


@task
def _total_res_buildings_by_sa(
    gdf: gpd.GeoDataFrame, on: str, renamed: str,
) -> pd.DataFrame:

    return gdf[[on]].value_counts().rename(renamed).reset_index()


@task
def _count_buildings_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].value_counts(normalize=True).rename(renamed).reset_index()


@task
def _final_count(df: pd.DataFrame, total: str, count: str, ratio: str) -> pd.DataFrame:

    df[total] = df[count] * df[ratio]

    return df


with Flow("Create synthetic residential building stock") as flow:

    dublin_sa = _read_sa_parquet(PROCESSED_DIR / "small_area_geometries_2016.parquet")
    sa_geo = _read_sa_geometries(PROCESSED_DIR / "small_area_geometries_2016.parquet")
    ber = _read_csv(RAW_DIR / "BER.09.06.2020.csv")
    ber_dublin = _merge_ber_sa(
        left=dublin_sa,
        right=ber,
        left_on="small_area",
        right_on="cso_small_area",
        how="inner",
        indicator=True,
    )
    geo = _read_csv(RAW_DIR / "DublinBuildingsData.csv")
    geo_transformed = _transform_res(df=geo, lon=geo["LONGITUDE"], lat=geo["LATITUDE"])
    geo_extracted = _extract_res(df=geo_transformed, on="BUILDING_USE", value="R")
    geo_joined = _spatial_join(sa_geo, geo_extracted, how="right")
    ber_assigned = _assign_building_type(
        ber_dublin,
        on="Dwelling type description",
        equiv={
            "Mid floor apt.": "Apartment",
            "Top-floor apt.": "Apartment",
            "Apt.": "Apartment",
            "Maisonette": "Apartment",
            "Grnd floor apt.": "Apartment",
            "Semi-det. house": "Semi detatched house",
            "House": "Semi detatched house",
            "Det. house": "Detatched house",
            "Mid terrc house": "Terraced house",
            "End terrc house": "Terraced house",
            "None": "Not stated",
        },
    )
    geo_total = _total_res_buildings_by_sa(
        geo_joined, on="small_area", renamed="total_dwellings",
    )
    ber_pre = _split_ber_construction(
        df=ber_assigned, condition="`Year of construction`<1973",
    )
    ber_post = _split_ber_construction(
        df=ber_assigned, condition="`Year of construction`>1972",
    )
    ber_pre_grouped = _count_buildings_by_sa(
        ber_pre,
        by="cso_small_area",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )
    ber_post_grouped = _count_buildings_by_sa(
        ber_post,
        by="cso_small_area",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )
    joined_pre = _merge_ber_sa(
        left=geo_total,
        right=ber_pre_grouped,
        left_on="small_area",
        right_on="cso_small_area",
        how="inner",
    )
    joined_post = _merge_ber_sa(
        left=geo_total,
        right=ber_post_grouped,
        left_on="small_area",
        right_on="cso_small_area",
        how="inner",
    )
    output_pre = _final_count(
        joined_pre,
        total="total_buildings_per_sa",
        count="total_dwellings",
        ratio="Dwelling Percentage",
    )
    output_post = _final_count(
        joined_post,
        total="total_buildings_per_sa",
        count="total_dwellings",
        ratio="Dwelling Percentage",
    )
