from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
from icontract import ensure, require
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
from pipeop import pipes
import prefect
from prefect import Flow, task
from prefect.core.task import Task
from prefect.utilities.debug import raise_on_exception
from toolz.functoolz import pipe
from unidecode import unidecode


# Utility Functions
# -----------------


@task
def _load_csv(filepath: Path) -> pd.DataFrame:

    return pd.read_csv(filepath)


@task
def _load_geodataframe(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def _save_to_parquet(census: pd.DataFrame, filepath: Path) -> None:

    census.to_parquet(filepath)


# Period Built
# ------------


@task
def _extract_columns_period_built_data(census: pd.DataFrame) -> pd.DataFrame:

    return census[["SMALL_AREA", "Period Built", "Households", "Persons"]]


@task
def _rename_columns_period_built_data(census: pd.DataFrame) -> pd.DataFrame:

    return census.rename(
        columns={
            "SMALL_AREA": "sas",
            "Period Built": "period_built",
            "Households": "all_households",
            "Persons": "persons",
        }
    )


@task
def _rename_period_built_to_same_as_census(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(
        period_built=census["period_built"]
        .str.lower()
        .replace({"pre 1919": "before 1919"})
    )


# Dwelling Type
# -------------


@task
def _extract_columns_dwelling_type_data(census: pd.DataFrame) -> pd.DataFrame:

    return census[["SMALL_AREA", "Type of accommodation", "Households", "Persons"]]


@task
def _rename_columns_dwelling_type_data(census: pd.DataFrame) -> pd.DataFrame:

    return census.rename(
        columns={
            "SMALL_AREA": "sas",
            "Type of accommodation": "dwelling_type",
            "Households": "all years",
            "Persons": "persons",
        }
    )


@task
def _rename_dwelling_type_to_same_as_census(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(
        dwelling_type=census["dwelling_type"]
        .replace(
            {
                "House/Bungalow": "house",
                "Flat/Apartment": "apartment",
                "Total": "all_households",
            }
        )
        .str.lower()
    )


@task
def _drop_rows_period_built_data(census: pd.DataFrame) -> pd.DataFrame:

    mask = np.isin(census["dwelling_type"], ["bed-sit", "caravan/mobile home"])
    return census[~mask]


# Link Period Built & Dwelling Type
# ---------------------------------


@task
def _merge_dwelling_type_with_period_built_data(
    dwelling_type: pd.DataFrame,
) -> pd.DataFrame:

    merged_columns = pd.MultiIndex.from_product(
        [
            ["all_households", "persons"],
            ["all_households", "house", "apartment", "not_stated"],
        ]
    )

    merged_data = pd.DataFrame(
        index=period_built.set_index(["sas", "period_built"]).index,
        columns=merged_columns,
    )

    merged.loc[:, pd.IndexSlice[:, "all_households"]] = period_built[
        ["all_households", "persons"]
    ].values

    pivoted_dwelling_type_for_merge = (
        dwelling_type.set_index(["sas", "dwelling_type"]).stack().unstack(1)
    )


@task
def _link_period_built_and_dwelling_type_data(
    period_built: pd.DataFrame, dwelling_type: pd.DataFrame,
) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()


# Link to EDs
# -----------


@task
def _link_census_to_eds(census: pd.DataFrame, sas: gpd.GeoDataFrame) -> pd.DataFrame:

    return pd.merge(census, sas[["sas", "eds"]], on="sas")


""" NOTE:

    logger = prefect.context.get("logger")
    logger.info(census)

"""
