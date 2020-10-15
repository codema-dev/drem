import re

from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task

import drem.utilities.pandas_tasks as pdt

from drem.utilities.visualize import VisualizeMixin


standardise_postcode_names = pdt.ReplaceSubstringInColumn(
    name="Delete all county names starting with a 0 followed by a number",
)
standardise_co_dublin_string = pdt.Replace(
    name="Replace 'Dublin County' with 'Co. Dublin'",
)
join_postcodes_to_counties = pdt.Concat(
    name="Join County-level Data to Postcode-level Data",
)
link_postcode_demands_to_boiler_totals = pdt.Merge(
    name="Link Postcode Demands to Dwelling Boiler Totals",
)
link_postcode_demands_to_postcode_geometries = pdt.Merge(
    name="Link Postcode Demands to Postcode Geometries",
)
calc_gas_boiler_total_for_each_postcode = pdt.GroupbySum(
    name="Calculate the total number of boilers in each Postcode",
)
rename_boiler_total_as_gas_hh_total = pdt.Rename(name="Rename 'total' to 'gas_hh_2016'")


@task(name="Replace column names with values from the row")
def _replace_column_names_with_third_row(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    columns = df.iloc[2].tolist()
    df.columns = columns

    return df.drop(index=[0, 1, 2]).reset_index(drop=True)


with Flow("Transform CSO Residential Network Gas Data") as flow:

    fpath = Parameter("fpath")
    dublin_pcodes = Parameter("dublin_pcodes")
    sa_boiler_stats = Parameter("sa_boiler_stats")

    raw_gas_tables = pdt.read_html(fpath)

    # Residential Postcode Annual Gas Consumption
    # -------------------------------------------
    table_number_of_resid_annual_gas_by_pcode = 11
    raw_resid_annual_gas_by_pcode = raw_gas_tables[
        table_number_of_resid_annual_gas_by_pcode
    ]
    resid_annual_gas_by_pcode_col_names_replaced = _replace_column_names_with_third_row(
        raw_resid_annual_gas_by_pcode,
    )
    resid_annual_gas_by_pcode_standardised = standardise_postcode_names(
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

    # Residential County Annual Gas Consumption
    # -----------------------------------------
    table_number_of_resid_annual_gas_by_county = 9
    raw_resid_annual_gas_by_county = raw_gas_tables[
        table_number_of_resid_annual_gas_by_county
    ]
    resid_annual_gas_by_county_col_names_replaced = _replace_column_names_with_third_row(
        raw_resid_annual_gas_by_county,
    )
    resid_annual_gas_by_county_standardised = standardise_co_dublin_string(
        resid_annual_gas_by_county_col_names_replaced,
        target="County",
        result="postcodes",
        to_replace="Dublin County",
        value="Co. Dublin",
    )
    resid_annual_gas_by_county_and_pcode = join_postcodes_to_counties(
        objs=[
            resid_annual_gas_by_pcode_standardised,
            resid_annual_gas_by_county_standardised,
        ],
        axis="index",
    )

    # Residential Postcode Gas Boiler Statistics
    # ------------------------------------------
    gas_boilers_extracted = pdt.get_rows_where_column_contains_substring(
        sa_boiler_stats, target="boiler_type", substring="Natural gas",
    )
    gas_boilers_by_postcode = calc_gas_boiler_total_for_each_postcode(
        gas_boilers_extracted, by="postcodes", target="total",
    )
    gas_boilers_renamed = rename_boiler_total_as_gas_hh_total(
        gas_boilers_by_postcode, columns={"total": "gas_hh_2016"},
    )
    resid_gas_with_boiler_totals = link_postcode_demands_to_boiler_totals(
        resid_annual_gas_by_county_and_pcode, gas_boilers_renamed, how="left",
    )

    # Non-residential Annual Gas Consumption
    # --------------------------------------
    table_number_of_non_resid_annual_gas_by_pcode = 10
    raw_non_resid_annual_gas_by_pcode = raw_gas_tables[
        table_number_of_non_resid_annual_gas_by_pcode
    ]
    non_resid_annual_gas_by_pcode_col_names_replaced = _replace_column_names_with_third_row(
        raw_non_resid_annual_gas_by_pcode,
    )
    non_resid_annual_gas_by_pcode_standardised = standardise_postcode_names(
        non_resid_annual_gas_by_pcode_col_names_replaced,
        target="Dublin Postal District",
        result="postcodes",
        pat=r"""     # Replace all substrings
            0       # starting with 0
            (?=\d)  # followed by a number
            """,
        repl="",  # with an empty string
        flags=re.VERBOSE,
    )

    # Non-residential County Annual Gas Consumption
    # ---------------------------------------------
    table_number_of_non_resid_annual_gas_by_county = 8
    raw_non_resid_annual_gas_by_county = raw_gas_tables[
        table_number_of_resid_annual_gas_by_county
    ]
    non_resid_annual_gas_by_county_col_names_replaced = _replace_column_names_with_third_row(
        raw_non_resid_annual_gas_by_county,
    )
    non_resid_annual_gas_by_county_standardised = standardise_co_dublin_string(
        non_resid_annual_gas_by_county_col_names_replaced,
        target="County",
        result="postcodes",
        to_replace="Dublin County",
        value="Co. Dublin",
    )
    non_resid_annual_gas_by_county_and_pcode = join_postcodes_to_counties(
        objs=[
            non_resid_annual_gas_by_pcode_standardised,
            non_resid_annual_gas_by_county_standardised,
        ],
        axis="index",
    )

    # Postcode Geometries
    # -------------------
    resid_gas_with_postcode_geometries = link_postcode_demands_to_postcode_geometries(
        dublin_pcodes, resid_gas_with_boiler_totals, how="left",
    )
    non_resid_gas_with_postcode_geometries = link_postcode_demands_to_postcode_geometries(
        dublin_pcodes, non_resid_annual_gas_by_county_and_pcode, how="left",
    )


class TransformCSOGas(Task, VisualizeMixin):
    """Transform CSO Gas via a Prefect flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(
        self,
        dirpath: Path,
        filename: str,
        dublin_postcodes: gpd.GeoDataFrame,
        small_area_boiler_statistics: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Run module Prefect flow.

        Args:
            dirpath (Path): Path to directory containing data
            filename (str): Name of CSO Gas html file
            dublin_postcodes (gpd.GeoDataFrame): Dublin Postcode Geometries
            small_area_boiler_statistics (pd.DataFrame): Small Area Boiler Statistics

        Returns:
            Dict[str, gpd.GeoDataFrame]: Dublin Gas Demand by Sector
        """
        filepath = dirpath / f"{filename}.html"
        state = self.flow.run(
            fpath=filepath,
            dublin_pcodes=dublin_postcodes,
            sa_boiler_stats=small_area_boiler_statistics,
        )
        return {
            "Residential": state.result[resid_gas_with_postcode_geometries].result,
            "Non-Residential": state.result[
                non_resid_gas_with_postcode_geometries
            ].result,
        }


transform_cso_gas = TransformCSOGas()
