from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
from icontract import ensure, require
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
from pipeop import pipes
from prefect import Flow, task
from prefect.core.task import Task
from prefect.utilities.debug import raise_on_exception
from toolz.functoolz import pipe
from unidecode import unidecode


from codema_drem.utilities.icontract import (
    no_null_values_in_result_dataframe,
    no_null_values_in_result_dataframe_columns,
    columns_are_in_dataframe,
)

np.seterr(all="raise")  # force numpy runtime errors to stop code


# Load Data
# ---------


@task
def _load_census2016_raw(filepath: Path) -> pd.DataFrame:

    return pd.read_excel(
        filepath, header=[1, 2], index_col=[0], skiprows=[3], engine="openpyxl",
    )


@task
def _load_geodataframe(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def _load_parquet(filepath: Path) -> pd.DataFrame:

    return pd.read_parquet(filepath)


@task
def _get_all_of_dublin_total(filepath: Path) -> pd.DataFrame:

    return pd.read_excel(
        filepath, header=[1, 2], skiprows=[0], nrows=1, engine="openpyxl"
    )


# Group scraped data by ed
# ------------------------


@task
def _group_period_built_data_by_ed(total_hh: pd.DataFrame) -> pd.DataFrame:

    return (
        total_hh.groupby(["eds", "period_built"])[["all_households", "persons"]]
        .sum()
        .reset_index()
    )


@task
def _group_dwelling_type_data_by_ed(total_hh: pd.DataFrame) -> pd.DataFrame:

    return (
        total_hh.groupby(["eds", "dwelling_type"])[["all_years", "persons"]]
        .sum()
        .reset_index()
    )


# Reformat Data
# -------------


def _pivot_dwelling_type_and_periods_built_to_rows(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census.stack().stack().reset_index()


def _rename_columns_to_dwelling_type_period_built_total_hh(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census.rename(
        columns={
            "Dublin City": "eds",
            "Census 2016": "period_built",
            "level_2": "dwelling_type",
            0: "total_hh",
        }
    )


@require(lambda census: "eds" in census.columns)
def _remove_numbers_from_eds(census: pd.DataFrame,) -> pd.DataFrame:

    census["eds"] = census["eds"].str.extract(r"\d+(.+)", expand=False)
    return census


def _rename_apartments(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(
        dwelling_type=census["dwelling_type"]
        .replace({"All Apartments and Bed-sits": "apartment"})
        .str.lower()
    )


def _set_columns_to_lowercase(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(
        eds=census["eds"].str.lower(),
        period_built=census["period_built"].str.lower(),
        dwelling_type=census["dwelling_type"].str.lower(),
    )


@task
def _reformat_data(census: pd.DataFrame) -> pd.DataFrame:

    return pipe(
        _pivot_dwelling_type_and_periods_built_to_rows(census),
        _rename_columns_to_dwelling_type_period_built_total_hh,
        _rename_apartments,
        _remove_numbers_from_eds,
        _set_columns_to_lowercase,
    )


# Re-used Utility functions
# ^^^^^^^^^^^^^^^^^^^^^^^^^


def _pivot_index_to_columns(index: str, census: pd.DataFrame) -> pd.DataFrame:

    return census.set_index(["eds", "dwelling_type", "period_built"])[
        "total_hh"
    ].unstack(index)


def _restack_columns_as_index(census: pd.DataFrame) -> pd.DataFrame:

    return census.stack().reset_index().rename(columns={0: "total_hh"})


def _pivot_all_years_scraped_to_columns(
    column: str, all_years_scraped: pd.DataFrame
) -> pd.DataFrame:

    return all_years_scraped.set_index(["eds", "dwelling_type"])[column].unstack(1)


# Replace '<?' values with integers
# --------------------------------------


@task
def _replace_all_less_than_six_values_with_zero(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(total_hh=census["total_hh"].replace({"<6": 0}))


@task
def _replace_all_households_with_scraped_all_households(
    census: pd.DataFrame, period_built_totals_scraped: pd.DataFrame,
) -> pd.DataFrame:

    census = _pivot_index_to_columns("dwelling_type", census)
    census = census.assign(
        all_households=period_built_totals_scraped["all_households"].values
    ).drop(columns="all households")

    return _restack_columns_as_index(census)


@task
def _replace_all_other_less_than_values_with_themselves(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census.assign(total_hh=census["total_hh"].astype(str).str.extract("(\d+)"))


# Set Column Data Types
# ---------------------


def _set_total_hh_data_type_to_int(census: pd.DataFrame) -> pd.DataFrame:

    return census.assign(total_hh=census["total_hh"].astype(np.int16))


@task
def _set_column_data_types(census: pd.DataFrame) -> pd.DataFrame:

    return _set_total_hh_data_type_to_int(census)


# Amalgamate house-types into one
# -------------------------------


@task
def _amalgamate_d_semid_terraced_into_house(census: pd.DataFrame) -> pd.DataFrame:

    census = _pivot_index_to_columns("dwelling_type", census)
    census["house"] = (
        census["detached house"]
        + census["semi-detached house"]
        + census["terraced house"]
    )
    census = census.drop(
        columns=["detached house", "semi-detached house", "terraced house"]
    )

    return _restack_columns_as_index(census)


# Replace all_years with scraped all_years
# ----------------------------------------


@task
def _replace_all_years_with_scraped_all_years(
    census: pd.DataFrame, all_years_scraped: pd.DataFrame,
) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    all_years_scraped = _pivot_all_years_scraped_to_columns(
        "all_years", all_years_scraped
    )
    census = _pivot_index_to_columns("period_built", census)


# Add column for number of persons
# --------------------------------


@task
def _add_column_for_number_of_persons_in_each_period_built_hh_from_scraped(
    census: pd.DataFrame, period_built_totals_scraped: pd.DataFrame,
) -> pd.DataFrame:

    census = _pivot_index_to_columns("dwelling_type", census)
    census = census.assign(persons=period_built_totals_scraped["persons"].values)

    return _restack_columns_as_index(census)


# Amalgamate dwelling_type houses to one category
# -----------------------------------------------


# Infer <6 values
# ---------------


def _count_groupings_with_values_less_than_six_by_ed(
    census: pd.DataFrame,
) -> pd.DataFrame:

    less_than_six_mask = census < 6
    return less_than_six_mask.sum(level=0)


def _calculate_sum_of_groupings_by_ed(
    index: str, ignore_row: str, census: pd.DataFrame
) -> pd.DataFrame:

    return census.loc[census.index.get_level_values(index) != ignore_row].sum(level=0)


def _select_row_by_ed(row: str, census: pd.DataFrame) -> pd.DataFrame:

    return census.loc[pd.IndexSlice[:, row], :].droplevel(1)


def _infer_values_so_sum_of_periods_built_is_approx_equal_to_all_years(
    census: pd.DataFrame,
) -> pd.DataFrame:
    """This function infers values for all values <6 so that the sum of 
    buildings is greater than or equal to the recorded total (i.e. before 1919,
    2011 or later etc. should be greater than or equal to all years)
    
    Parameters
    ----------
    census : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    census = _pivot_index_to_columns("dwelling_type", census)

    number_of_categories_with_values_less_than_six_by_ed = _count_groupings_with_values_less_than_six_by_ed(
        census
    )

    # What the sum of periods built should be...
    census_total_by_ed = _select_row_by_ed("all years", census)

    # What it is...
    census_sum_by_ed = _calculate_sum_of_groupings_by_ed(
        index="period_built", ignore_row="all years", census=census
    )

    difference = census_total_by_ed - census_sum_by_ed

    # Distribute the difference between among <6 values...
    to_add_to_less_than_six_values = (
        difference // number_of_categories_with_values_less_than_six_by_ed
    ).fillna(0)
    census = census.mask(
        census < 6, census[census < 6] + to_add_to_less_than_six_values, axis=0
    )

    return _restack_columns_as_index(census)


def _infer_values_so_sum_of_dwelling_types_is_approx_equal_to_all_households(
    census: pd.DataFrame,
) -> pd.DataFrame:
    """This function infers values for all values <6 so that the sum of 
    buildings is greater than or equal to the recorded total (i.e. apartments,
    detached, etc. should be greater than or equal to all households)
    
    Parameters
    ----------
    census : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    census = _pivot_index_to_columns("period_built", census)

    number_of_categories_with_values_less_than_six_by_ed = _count_groupings_with_values_less_than_six_by_ed(
        census
    )

    # What the sum of periods built should be...
    census_total_by_ed = _select_row_by_ed("all households", census)

    # What it is...
    census_sum_by_ed = _calculate_sum_of_groupings_by_ed(
        index="dwelling_type", ignore_row="all households", census=census,
    )

    difference = census_total_by_ed - census_sum_by_ed

    # Distribute the difference between among <6 values...
    to_add_to_less_than_six_values = (
        difference // number_of_categories_with_values_less_than_six_by_ed
    ).fillna(0)
    census = census.mask(
        census < 6, census[census < 6] + to_add_to_less_than_six_values, axis=0
    )

    return _restack_columns_as_index(census)


@task
def _infer_values_for_less_than_six_groupings(census: pd.DataFrame) -> pd.DataFrame:

    return pipe(
        _infer_values_so_sum_of_periods_built_is_approx_equal_to_all_years(census),
        _infer_values_so_sum_of_dwelling_types_is_approx_equal_to_all_households,
    )


# Infer not stated values
# -----------------------


def _distribute_not_stated_column(census: pd.DataFrame) -> pd.DataFrame:

    total = census.sum(axis=1).replace({0: 1}).values[:, np.newaxis]
    columns_as_percentage_of_total = census.values / total

    not_stated = census["not stated"].values[:, np.newaxis]
    not_stated_distributed = np.floor(columns_as_percentage_of_total * not_stated)
    census_with_not_stated_distributed = census.values + not_stated_distributed

    return pd.DataFrame(
        data=census_with_not_stated_distributed,
        columns=census.columns,
        index=census.index,
    )


@task
def _infer_values_for_not_stated_groupings(census: pd.DataFrame) -> pd.DataFrame:

    for category in ["dwelling_type", "period_built"]:
        census = pipe(
            _pivot_index_to_columns(category, census),
            _distribute_not_stated_column,
            _restack_columns_as_index,
        )

    return census


# Drop rows & columns
# -------------------


def _drop_not_stated_column(census: pd.DataFrame) -> pd.DataFrame:

    return census.drop(columns="not stated")


def _drop_column(census: pd.DataFrame, column: str) -> pd.DataFrame:

    return census.drop(columns=column)


@task
def _drop_totals_and_not_stated(census: pd.DataFrame) -> pd.DataFrame:

    for category, column in zip(
        ["dwelling_type", "period_built"], ["all households", "all years"]
    ):
        census = _pivot_index_to_columns(category, census)
        census = _drop_not_stated_column(census)
        census = _drop_column(census, column)
        census = _restack_columns_as_index(census)

    return census


# Replace negative values...
# --------------------------


@task
def _replace_negative_value_groupings_with_zero(census: pd.DataFrame,) -> pd.DataFrame:

    # NOTE: 1183 negative buildings replaced with zero!

    mask = census.total_hh < 0
    census.loc[mask] = 0
    return census


# Rename unmatched electoral districts
# ------------------------------------


@task
def _rename_electoral_districts_so_compatible_with_cso_map(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census.assign(
        eds=census["eds"]
        .astype(str)
        .apply(unidecode)  # replace fadas with letters
        .str.replace(pat=r"[']", repl="")  # replace accents with blanks
        .str.replace("foxrock-deans grange", "foxrock-deansgrange", regex=False,)
    )


@task
def _reset_column_data_types(census: pd.DataFrame,) -> pd.DataFrame:

    return census.assign(
        dwelling_type=census["dwelling_type"].astype(str),
        period_built=census["period_built"].astype(str),
    )


@task
def _save_dataframe_result(census: pd.DataFrame,) -> None:

    census.to_parquet(CENSUS_CLEAN)

