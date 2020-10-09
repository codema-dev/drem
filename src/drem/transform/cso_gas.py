import re

from pathlib import Path

import geopandas as gpd
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task

import drem.utilities.pandas_tasks as pdt


@task
def _replace_column_name_with_third_row(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    columns = df.iloc[2].tolist()
    df.columns = columns

    return df.drop(index=[0, 1, 2]).reset_index(drop=True)


with Flow("Transform CSO Residential Network Gas Data") as flow:

    fpath = Parameter("fpath")
    dublin_pcodes = Parameter("dublin_pcodes")

    raw_gas_tables = pdt.read_html(fpath)

    # Residential Annual Gas Consumption
    # ----------------------------------
    table_number_of_resid_annual_gas_by_pcode = 11
    raw_resid_annual_gas_by_pcode = raw_gas_tables[
        table_number_of_resid_annual_gas_by_pcode
    ]
    resid_annual_gas_by_pcode_col_names_replaced = _replace_column_name_with_third_row(
        raw_resid_annual_gas_by_pcode,
    )
    resid_annual_gas_by_pcode_standardised = pdt.replace_substring_in_column(
        resid_annual_gas_by_pcode_col_names_replaced,
        target="Dublin Postal District",
        result="postcodes",
        pat=r"""     # Replace all substrings
            0       # starting with 0
            (?=\d)  # followed by a number
            """,
        repl="",  # with an empty string
        flags=re.VERBOSE,
    )

    table_number_of_resid_annual_gas_by_county = 9
    raw_resid_annual_gas_by_county = raw_gas_tables[
        table_number_of_resid_annual_gas_by_county
    ]
    resid_annual_gas_by_county_col_names_replaced = _replace_column_name_with_third_row(
        raw_resid_annual_gas_by_county,
    )
    resid_annual_gas_by_county_standardised = pdt.replace(
        resid_annual_gas_by_county_col_names_replaced,
        target="County",
        result="postcodes",
        to_replace="Dublin County",
        value="Co. Dublin",
    )
    resid_annual_gas_by_county_and_pcode = pdt.concat(
        objs=[
            resid_annual_gas_by_pcode_standardised,
            resid_annual_gas_by_county_standardised,
        ],
        axis="index",
    )

    resid_gas_with_postcode_geometries = pdt.merge(
        dublin_pcodes, resid_annual_gas_by_county_and_pcode, how="left",
    )


class TransformCSOGas(Task):
    """Transform CSO Gas via a Prefect flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(
        self, dirpath: Path, filename: str, dublin_postcodes: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Run module Prefect flow.

        Args:
            dirpath (Path): Path to directory containing data
            filename (str): Name of CSO Gas html file
            dublin_postcodes (gpd.GeoDataFrame): Dublin Postcode Geometries

        Returns:
            Dict[str, gpd.GeoDataFrame]: Dublin Gas Demand by Sector
        """
        filepath = dirpath / f"{filename}.html"
        state = flow.run(fpath=filepath, dublin_pcodes=dublin_postcodes)
        return {
            "Residential": state.result[resid_gas_with_postcode_geometries].result,
        }


transform_cso_gas = TransformCSOGas()
