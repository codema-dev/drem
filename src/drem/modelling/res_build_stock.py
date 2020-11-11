"""

This script will create a synthetic residential building stock.
Our methodology is to divide each SA into several building types,
using the Split-Apply-Combine pandas transform. These ratios will
then be applied to outputs from EnergyPlus to generate a first-pass
estimate for residential energy demand in Dublin.

"""
import pandas as pd

from prefect import Flow
from prefect import task

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR


@task
def _read_sa_parquet(input_filepath: str) -> pd.DataFrame:

    return pd.read_parquet(input_filepath).drop_duplicates()


@task
def _read_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _merge_ber_sa(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _extract_res(df: pd.DataFrame, on: str, value: str) -> pd.DataFrame:

    return df.loc[df[on] == value]


@task
def _assign_building_type(df: pd.DataFrame, on: str, equiv: list) -> pd.DataFrame:

    return df.replace({on: equiv})


@task
def _count_buildings_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].value_counts(normalize=True).rename(renamed).reset_index()


with Flow("Create synthetic residential building stock") as flow:

    dublin_sa = _read_sa_parquet(PROCESSED_DIR / "small_area_geometries_2016.parquet")
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
    geo = _extract_res(geo, on="BUILDING_USE", value="R")
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
    ber_grouped = _count_buildings_by_sa(
        ber_assigned,
        by="cso_small_area",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )
