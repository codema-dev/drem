"""

This script will create a synthetic residential building stock to divide
each SA into several building types, using the Split-Apply-Combine pandas
transform. These ratios will then be applied to outputs from EnergyPlus
to generate a first-pass estimate for residential energy demand in Dublin

"""

import pandas as pd
import geopandas as gpd

from typing import Dict
from typing import List
from pathlib import Path

from prefect import Flow
from prefect import Parameter
from prefect import task

from drem.filepaths import RAW_DIR
from drem.filepaths import EXTERNAL_DIR


@task
def _read_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _assign_building_type(df: pd.DataFrame, on: str, equiv: list) -> pd.DataFrame:

    return df.replace({on: equiv})


@task
def _group_buildings_by_sa(
    df: pd.DataFrame, by: str, dwelling: str, renamed: str
) -> pd.DataFrame:

    return df.groupby(by)[dwelling]


@task
def _count_dwellings_by_sa(df: pd.DataFrame) -> pd.DataFrame:

    return df.iloc[:, 0].value_counts(normalize=True)


with Flow("Create synthetic residential building stock") as flow:

    ber = _read_csv(RAW_DIR / "BER.09.06.2020.csv")
    ber_assigned = _assign_building_type(
        ber,
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
    ber_grouped = _group_buildings_by_sa(
        ber_assigned,
        by="CSO_ED_ID",
        dwelling=["Dwelling type description"],
        renamed="Dwelling Percentage",
    )
    ber_counted = _count_dwellings_by_sa(ber_assigned)
