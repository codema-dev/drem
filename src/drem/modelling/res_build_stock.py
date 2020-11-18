"""
This script will create a synthetic residential building stock.

Our methodology is to divide each SA into several building types,
using the Split-Apply-Combine pandas transform. These ratios will
then be applied to outputs from EnergyPlus to generate a first-pass
estimate for residential energy demand in Dublin.

"""
import geopandas as gpd
import numpy as np
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
def _read_energy_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath)


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
def _apply_period_built_split(
    df: pd.DataFrame,
    sa: str,
    yc: str,
    split: str,
    year: int,
    post: str,
    pre: str,
    pre_post: str,
) -> pd.DataFrame:

    df = df.groupby([sa, yc])
    df = df.head(300000)
    df[yc] = df[yc].astype(int)
    df[split] = np.where(df[yc] >= year, post, pre)
    df = df.groupby(sa)[split].value_counts(normalize=True)
    df = df.to_frame()
    df = df.rename(columns={split: pre_post})

    return df.reset_index()


@task
def _count_buildings_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].value_counts(normalize=True).rename(renamed).reset_index()


@task
def _final_count(df: pd.DataFrame, total: str, count: str, ratio: str) -> pd.DataFrame:

    df[total] = df[count] * df[ratio]

    return df


@task
def _add_energy_description(
    df: pd.DataFrame, new: str, left: str, right: str,
) -> pd.DataFrame:

    df[new] = df[left] + df[right]

    return df


@task
def _calculate_energy_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].sum().rename(renamed).reset_index()


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
    energyplus = _read_energy_csv(PROCESSED_DIR / "energy_demand_by_building_type.csv")
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
    ber_split = _split_ber_construction(
        df=ber_assigned, condition="`Year of construction`<1973",
    )
    period_built_split = _apply_period_built_split(
        df=ber_assigned,
        sa="small_area",
        yc="Year of construction",
        split="yc_split",
        year=1973,
        post="post",
        pre="pre",
        pre_post="pre_post",
    )
    ber_grouped = _count_buildings_by_sa(
        ber_split,
        by="cso_small_area",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )
    joined = _merge_ber_sa(
        left=geo_total,
        right=ber_grouped,
        left_on="small_area",
        right_on="cso_small_area",
        how="inner",
    )
    output = _final_count(
        joined,
        total="total_buildings_per_sa",
        count="total_dwellings",
        ratio="Dwelling Percentage",
    )
    output_split = _merge_ber_sa(
        left=period_built_split,
        right=output,
        left_on="small_area",
        right_on="small_area",
        how="inner",
    )
    output_dataframe = _final_count(
        output_split,
        total="total_sa_final",
        count="total_buildings_per_sa",
        ratio="pre_post",
    )
    output_added = _add_energy_description(
        df=output_dataframe,
        new="building_energyplus",
        left="Dwelling type description",
        right="yc_split",
    )
    output_merged = _merge_ber_sa(
        left=output_added,
        right=energyplus,
        left_on="building_energyplus",
        right_on="dwelling",
        how="inner",
    )
    output_energy = _final_count(
        df=output_merged,
        total="energy_kwh",
        count="total_sa_final",
        ratio="total_site_energy_kwh",
    )
    energy_sa = _calculate_energy_by_sa(
        df=output_energy, by="small_area", on="energy_kwh", renamed="energy_per_sa_kwh",
    )
