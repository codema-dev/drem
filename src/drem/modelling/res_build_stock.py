"""

This script will create a synthetic residential building stock to divide
each SA into several building types. These ratios will then be applied to
outputs from EnergyPlus to generate a first-pass estimate for residential
energy demand in Dublin

"""

import pandas as pd
import geopandas as gpd

from pathlib import Path

from prefect import Flow
from prefect import Parameter
from prefect import task

from drem.filepaths import RAW_DIR
from drem.filepaths import EXTERNAL_DIR

@task 
def _read_csv(input_filepath: Path) -> pd.DataFrame

    return pd.read_csv:(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _assign_building_type(df: pd.DataFrame, on:str) -> pd.DataFrame

    return df[on].map:(
        {
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
        }
    )

@task
def _group_buildings_by_sa(df: pd.DataFrame,cso:str, dwelling:str, renamed:str) -> pd.DataFrame

return ber_closed.groupby('CSO_ED_ID')['Dwelling type description'].value_counts(normalize=True).rename("Dwelling Percentage")



with Flow as flow:


