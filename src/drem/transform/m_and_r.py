"""Merge MPRNs and GPRNs in the Monitoring & Reporting (m_and_r) data set.

- Standardise addresses using `pypostal`.
- Deduplicate standardised addresses to eliminate typos using `string_grouper`

Note: This module is not included in the prefect pipeline or tested as this would
require including libpostal in CI which would add a 2-3GB overhead...
"""

from pathlib import Path
from typing import Dict
from typing import List

import pandas as pd

from icontract import require
from postal.expand import expand_address
from prefect import Flow
from prefect import task
from string_grouper import group_similar_strings

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR


@task
def _read_parquet_file(filepath: Path) -> pd.DataFrame:

    return pd.read_parquet(filepath)


@task
@require(lambda df, column_names: set(column_names).issubset(set(df.columns)))
def _aggregate_columns(
    df: pd.DataFrame, column_names: List[str], to_column: str,
) -> pd.DataFrame:

    df[to_column] = df[column_names].agg(", ".join, axis="columns")

    return df


@task
@require(lambda addresses: "Location" in addresses.columns)
@require(lambda addresses: "address" not in addresses.columns)
def _standardise_addresses(
    addresses: pd.DataFrame, on_column: str, to_column: str,
) -> pd.DataFrame:

    addresses[to_column] = addresses[on_column].apply(
        lambda cell: expand_address(cell)[0],
    )

    return addresses


@task
@require(lambda df, mapping: set(mapping.keys()).issubset(set(df.columns)))
def _rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:

    return df.rename(columns=mapping)


@task
@require(lambda df, column_names: set(column_names).issubset(set(df.columns)))
def _extract_columns(df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:

    return df.loc[:, column_names]


@task
@require(lambda df, on: set(on).issubset(set(df.columns)))
@require(lambda df, target: target in df.columns)
def _sum_energies_for_multiple_entry_addresses(
    df: pd.DataFrame, on: List[str], target: str, result: str,
) -> pd.DataFrame:
    """Sum energies across sub-buildings to one representative building.

    Some buildings such as `Peamount Hospital` contain multiple MPRN and GPRN entries
    for each unique year and address; this function sums these energies into a single
    energy for `Peamount Hospital`

    Args:
        df (pd.DataFrame): MPRN or GPRN data
        on (List[str]): Columns on which to group buildings (typically Year, Address)
        target (str): Existing column to be summated
        result (str): New column to store results of summation

    Returns:
        pd.DataFrame: [description]
    """
    df[result] = df.groupby(on).sum(target).reset_index(drop=True)

    return df


@task
@require(lambda df, target: target in df.columns)
def _dedupe_column(df: pd.DataFrame, target: str, result: str) -> pd.DataFrame:
    """Deduplicate similar strings in column.

    Will group and rename similar strings such as 'leinster house' and 'lenister house'
    under a single spelling.

    Args:
        df (pd.DataFrame): DataFrame containing column to be deduped
        target (str): Column in DataFrame to be deduped
        result (str): New column containing deduplicated result of target column

    Returns:
        pd.DataFrame: DataFrame containing deduped column
    """
    df[result] = group_similar_strings(df[target], min_similarity=0.95)

    return df


@task
@require(lambda df, on: set(on).issubset(set(df.columns)))
def _drop_duplicates(df: pd.DataFrame, on: List[str]) -> pd.DataFrame:

    return df.drop_duplicates(subset=on)


@task
@require(lambda mprn, on: set(on).issubset(set(mprn.columns)))
@require(lambda gprn, on: set(on).issubset(set(gprn.columns)))
def _merge_mprn_and_gprn_on_common_addresses(
    mprn: pd.DataFrame, gprn: pd.DataFrame, on: List[str], **kwargs,
) -> pd.DataFrame:

    return mprn.merge(gprn, on=on, **kwargs)


@task
def _save_to_parquet_file(df: pd.DataFrame, filepath: Path) -> None:

    df.to_parquet(filepath)


with Flow("Merge MPRN and GPRN") as flow:

    mprn_raw = _read_parquet_file(RAW_DIR / "mprn.parquet")
    gprn_raw = _read_parquet_file(RAW_DIR / "gprn.parquet")

    mprn_aggregated = _aggregate_columns(
        mprn_raw, column_names=["PB Name", "Location"], to_column="combined_address",
    )
    gprn_aggregated = _aggregate_columns(
        gprn_raw, column_names=["PB Name", "Location"], to_column="combined_address",
    )
    mprn_renamed = _rename_columns(
        mprn_aggregated,
        {"Attributable Total Final Consumption (kWh)": "electricity_demand_kwh_year"},
    )
    gprn_renamed = _rename_columns(
        gprn_aggregated,
        {"Attributable Total Final Consumption (kWh)": "gas_demand_kwh_year"},
    )
    mprn_with_addresses = _standardise_addresses(
        mprn_renamed, on_column="combined_address", to_column="standardised_address",
    )
    gprn_with_addresses = _standardise_addresses(
        gprn_renamed, on_column="combined_address", to_column="standardised_address",
    )
    mprn_address_deduped = _dedupe_column(
        mprn_with_addresses, target="standardised_address", result="deduped_address",
    )
    gprn_address_deduped = _dedupe_column(
        gprn_with_addresses, target="standardised_address", result="deduped_address",
    )
    mprn_summated = _sum_energies_for_multiple_entry_addresses(
        mprn_address_deduped,
        on=["standardised_address", "Year"],
        target="electricity_demand_kwh_year",
        result="summated_electricity_demand_kwh_year",
    )
    gprn_summated = _sum_energies_for_multiple_entry_addresses(
        gprn_address_deduped,
        on=["standardised_address", "Year"],
        target="gas_demand_kwh_year",
        result="summated_gas_demand_kwh_year",
    )
    mprn_deduped = _drop_duplicates(mprn_summated, on=["standardised_address", "Year"])
    gprn_deduped = _drop_duplicates(gprn_summated, on=["standardised_address", "Year"])
    gprn_extracted = _extract_columns(
        gprn_summated,
        [
            "deduped_address",
            "standardised_address",
            "Year",
            "summated_gas_demand_kwh_year",
        ],
    )

    m_and_r = _merge_mprn_and_gprn_on_common_addresses(
        mprn_deduped,
        gprn_extracted,
        on=["standardised_address", "Year"],
        how="left",
        indicator=True,
    )

    _save_to_parquet_file(m_and_r, PROCESSED_DIR / "m_and_r.parquet")
