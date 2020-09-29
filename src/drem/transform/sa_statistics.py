from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Union

import geopandas as gpd
import pandas as pd

from icontract import require
from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception


@task
def _extract_rows_from_glossary(
    glossary: pd.DataFrame, target: str, table_name: str,
) -> pd.DataFrame:

    non_empty_rows = glossary[glossary[target].notna()].reset_index()

    # The relevant table rows always start one row above the table_name
    start_of_table = (
        non_empty_rows.query(f"`{target}` == '{table_name}'").index.item() - 1
    )
    next_non_empty_row: int = start_of_table + 2
    start_index: int = non_empty_rows.iloc[start_of_table]["index"]
    end_index: int = non_empty_rows.iloc[next_non_empty_row]["index"]

    return glossary.iloc[start_index:end_index].reset_index(drop=True)


@task
def _convert_columns_to_dict(
    extracted_table: pd.DataFrame, column_name_index: str, column_name_values: str,
) -> Dict[str, str]:

    return (
        extracted_table[[column_name_index, column_name_values]]
        .set_index(column_name_index)
        .to_dict()[column_name_values]
    )


@task
@require(lambda statistics, glossary: set(glossary.keys()).issubset(statistics.columns))
@require(
    lambda statistics, additional_columns: set(additional_columns).issubset(
        statistics.columns,
    ),
)
def _extract_column_names_via_glossary(
    statistics: pd.DataFrame, glossary: Dict[str, str], additional_columns: List[str],
) -> pd.DataFrame:

    columns_to_extract = additional_columns + list(glossary.keys())
    return statistics.loc[:, columns_to_extract]


@task
def _rename_columns_via_glossary(
    statistics: pd.DataFrame, glossary: Dict[str, str],
) -> pd.DataFrame:

    return statistics.rename(columns=glossary)


@task
def _melt_columns(df: pd.DataFrame, id_vars: List[str], **kwargs: Any) -> pd.DataFrame:
    """Melt columns into rows.

    Example:
        Before:
                  GEOGID  Pre 1919 (No. of households)   ...
        SA2017_017001001                            10   ...

        After:
                 GEOGID                       variable   value
        SA2017_017001001  Pre 1919 (No. of households)      10
                                                ...        ...

    Args:
        df (pd.DataFrame): Data to be melted
        id_vars (List[str]): Name of ID columns
        **kwargs (Any): passed to
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.melt.html

    Returns:
        pd.DataFrame: [description]
    """
    return df.melt(id_vars=id_vars, **kwargs)


@task
def _split_column_in_two_on_substring(
    df: pd.DataFrame,
    target: str,
    pat: str,
    left_column_name: str,
    right_column_name: str,
) -> pd.DataFrame:

    df[[left_column_name, right_column_name]] = df[target].str.split(
        pat=pat, expand=True,
    )

    return df


@task
def _replace_substring_in_column(
    df: pd.DataFrame, target: str, result: str, pat: str, repl: str, **kwargs: Any,
) -> pd.DataFrame:

    df[result] = df[target].str.replace(pat=pat, repl=repl, **kwargs)

    return df


@task
def _strip_column(
    df: pd.DataFrame, target: str, result: str, **kwargs: Any,
) -> pd.DataFrame:

    df[result] = df[target].str.strip(**kwargs)

    return df


@task
def _pivot(
    df: pd.DataFrame, values: Iterable[str], columns: Iterable[str], **kwargs: Any,
) -> pd.DataFrame:

    return df.pivot(columns=columns, values=values, **kwargs)


@task
def _concat(objs: Union[Iterable[pd.DataFrame]], **kwargs: Any) -> pd.DataFrame:

    return pd.concat(objs, **kwargs)


@task
def _merge_with_geometries(
    df: pd.DataFrame, geometries: gpd.GeoDataFrame, on: Iterable[str], **kwargs: Any,
) -> pd.DataFrame:

    return geometries.merge(df, on=on, **kwargs)


@task
def _link_small_areas_to_postcodes(
    small_area_statistics: gpd.GeoDataFrame, postcode_geometries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Link Small Areas to their corresponding Postcode.

    By finding which Postcode contains which Small Area Centroid.

    Args:
        small_area_statistics (gpd.GeoDataFrame): Statistics data containing small area
        geometries
        postcode_geometries (gpd.GeoDataFrame): Postcode geometries

    Returns:
        gpd.GeoDataFrame: Statistics data with small areas linked to postcodes
    """
    small_area_centroids = small_area_statistics.copy().assign(
        geometry=lambda gdf: gdf.geometry.centroid,
    )
    small_areas_linked_to_postcodes = gpd.sjoin(
        small_area_centroids, postcode_geometries, how="left",
    ).drop(columns=["index_right", "geometry"])

    return small_areas_linked_to_postcodes.assign(
        geometry=small_area_statistics.geometry,
    )


@task
@require(
    lambda df, columns: set(columns) <= set(df.columns),
    "df.columns doesn't contain all names in columns!",
)
def _get_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:

    return df.copy().loc[:, columns]


with Flow("Transform Dublin Small Area Statistics") as flow:

    raw_glossary = Parameter("raw_sa_glossary")
    raw_sa_stats = Parameter("raw_sa_stats")
    dublin_sa_geom = Parameter("dublin_sa_geom")
    dublin_pcodes = Parameter("dublin_pcodes")

    raw_year_built_glossary = _extract_rows_from_glossary(
        raw_glossary,
        target="Tables Within Themes",
        table_name="Permanent private households by year built ",
    )
    year_built_glossary = _convert_columns_to_dict(
        raw_year_built_glossary,
        column_name_index="Column Names",
        column_name_values="Description of Field",
    )

    raw_year_built_stats = _extract_column_names_via_glossary(
        raw_sa_stats, year_built_glossary, additional_columns=["GEOGID"],
    )
    year_built_stats_with_col_names_decoded = _rename_columns_via_glossary(
        raw_year_built_stats, year_built_glossary,
    )
    year_built_stats_with_columns_melted = _melt_columns(
        year_built_stats_with_col_names_decoded, id_vars=["GEOGID"],
    )
    year_built_stats_with_column_split = _split_column_in_two_on_substring(
        year_built_stats_with_columns_melted,
        target="variable",
        pat=r"(",
        left_column_name="raw_period_built",
        right_column_name="raw_households_and_persons",
    )
    year_built_stats_with_substring_no_of_replaced = _replace_substring_in_column(
        year_built_stats_with_column_split,
        target="raw_households_and_persons",
        result="households_and_persons",
        pat=r"(No. of )|(\))",
        repl="",
    )
    year_built_stats_with_substring_year_built_replaced = _replace_substring_in_column(
        year_built_stats_with_column_split,
        target="GEOGID",
        result="small_area",
        pat=r"(SA2017_)",
        repl="",
    )
    year_built_stats_with_col_whitespace_stripped = _strip_column(
        year_built_stats_with_substring_year_built_replaced,
        target="raw_period_built",
        result="period_built",
    )
    persons_and_hh_columns = _pivot(
        year_built_stats_with_col_whitespace_stripped,
        values="value",
        columns="households_and_persons",
    )
    year_built_stats_with_hh_and_people_split = _concat(
        [year_built_stats_with_col_whitespace_stripped, persons_and_hh_columns],
        axis="columns",
    )
    year_built_with_dublin_sa_geometries = _merge_with_geometries(
        year_built_stats_with_hh_and_people_split, dublin_sa_geom, on=["small_area"],
    )
    year_built_with_postcodes = _link_small_areas_to_postcodes(
        year_built_with_dublin_sa_geometries, dublin_pcodes,
    )
    clean_year_built = _get_columns(
        year_built_with_postcodes,
        columns=[
            "small_area",
            "postcodes",
            "period_built",
            "households",
            "persons",
            "geometry",
        ],
    )


class TransformSaStatistics(Task):
    """Transform Small Area Statistics.

    Args:
        Task (Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(
        self,
        raw_sa_glossary: pd.DataFrame,
        raw_sa_statistics: pd.DataFrame,
        dublin_postcodes: gpd.GeoDataFrame,
        dublin_sa_geometries: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Run local flow.

        Args:
            raw_sa_glossary (pd.DataFrame): Raw Small Area Glossary
            raw_sa_statistics (pd.DataFrame): Raw Ireland Small Area Statistics
            dublin_postcodes (pd.DataFrame): Dublin Postcode Geometries
            dublin_sa_geometries (pd.DataFrame): Dublin Small Area Geometries

        Returns:
            gpd.GeoDataFrame: Clean Dublin Small Area Statistics
        """
        with raise_on_exception():
            state = flow.run(
                parameters=dict(
                    raw_sa_glossary=raw_sa_glossary,
                    raw_sa_stats=raw_sa_statistics,
                    dublin_pcodes=dublin_postcodes,
                    dublin_sa_geom=dublin_sa_geometries,
                ),
            )

        return state.result[clean_year_built].result
