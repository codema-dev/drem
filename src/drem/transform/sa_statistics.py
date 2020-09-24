from typing import Dict
from typing import List

import geopandas as gpd
import pandas as pd

from icontract import ensure
from icontract import require
from prefect import task


def _extract_rows_from_glossary(
    glossary: pd.DataFrame, target: str, table_name: str, number_of_rows: int,
) -> pd.DataFrame:

    row_number_corresponding_to_table_name = glossary.query(
        f"`{target}` == '{table_name}'",
    ).index.item()

    # The relevant table rows always start one row above the table_name
    start_row: int = row_number_corresponding_to_table_name - 1
    end_row: int = start_row + number_of_rows

    return glossary.iloc[start_row:end_row].reset_index(drop=True)


@require(lambda columns: len(columns) == 2)
def _convert_columns_to_dict(
    extracted_table: pd.DataFrame, columns: str,
) -> Dict[str, str]:

    return extracted_table[columns].set_index(columns[0]).to_dict()[columns[1]]


def _extract_column_names_via_glossary(
    statistics: pd.DataFrame, glossary: Dict[str, str], additional_columns: List[str],
) -> pd.DataFrame:

    columns_to_extract = additional_columns + list(glossary.keys())
    return statistics.loc[:, columns_to_extract]


def _rename_columns_via_glossary(
    statistics: pd.DataFrame, glossary: Dict[str, str],
) -> pd.DataFrame:

    return statistics.rename(columns=glossary)


def _melt_year_built_columns(df: pd.DataFrame) -> pd.DataFrame:

    hh_columns: List[str] = [col for col in df.columns if "households" in col]
    person_columns: List[str] = [col for col in df.columns if "persons" in col]

    hh: pd.DataFrame = pd.melt(
        df,
        value_vars=hh_columns,
        id_vars="GEOGID",
        var_name="period_built",
        value_name="households",
    )
    persons: pd.DataFrame = pd.melt(
        df, value_vars=person_columns, id_vars="GEOGID", value_name="people",
    ).drop(columns=["GEOGID", "variable"])

    return pd.concat([hh, persons], axis=1)


def _clean_year_built_columns(df: pd.DataFrame) -> pd.DataFrame:

    return (
        df.copy()
        .assign(small_areas=lambda x: x["GEOGID"].str.replace(r"SA2017_", ""))
        .assign(
            period_built=lambda x: x["period_built"].str.replace(
                r" \(No. of households\)", "",
            ),
        )
        .drop(columns="GEOGID")
        .rename(columns={"small_areas": "small_area"})
        .assign(small_area=lambda x: x["small_area"].astype(str))
    )


@require(lambda statistics: "small_area" in statistics.columns)
@require(
    lambda geometries: set(geometries.columns) > {"small_area", "geometry"}
    or set(geometries.columns) == {"small_area", "geometry"},
)
def _extract_dublin_small_areas(
    statistics: pd.DataFrame, geometries: gpd.GeoDataFrame,
) -> pd.DataFrame:

    return geometries.copy().merge(statistics).drop(columns="geometry")


@require(lambda statistics: "small_area" in statistics.columns)
@require(
    lambda geometries: set(geometries.columns) > {"small_area", "geometry"}
    or set(geometries.columns) == {"small_area", "geometry"},
)
def _link_dublin_small_areas_to_geometries(
    statistics: pd.DataFrame, geometries: gpd.GeoDataFrame,
) -> pd.DataFrame:

    return geometries[["small_area", "geometry"]].copy().merge(statistics)


@ensure(
    lambda result: set(result.columns)
    == {"small_area", "period_built", "households", "people", "postcodes", "geometry"},
)
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


@task(name="Transform CSO Small Area Statistics via Glossary")
def transform_sa_statistics(
    statistics: pd.DataFrame,
    raw_glossary: pd.DataFrame,
    sa_geometries: gpd.GeoDataFrame,
    postcodes: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Transform CSO Small Area Statistics.

    Args:
        statistics (pd.DataFrame): CSO Small Area Statistics
        raw_glossary (pd.DataFrame): CSO Small Area Statistics Glossary
        sa_geometries (gpd.GeoDataFrame): CSO Small Area Geometries
        postcodes (gpd.GeoDataFrame): Dublin Postcode Geometries

    Returns:
        pd.DataFrame: Small Area Statistics in 'tidy-data' format
    """
    raw_year_built_glossary = _extract_rows_from_glossary(
        raw_glossary,
        target="Tables Within Themes",
        table_name="Permanent private households by year built ",
        number_of_rows=22,
    )
    year_built_glossary = _convert_columns_to_dict(
        raw_year_built_glossary, ["Tables Within Themes", "Description of Field"],
    )

    return (
        statistics.pipe(
            _extract_column_names_via_glossary,
            year_built_glossary,
            additional_columns=["GEOGID"],
        )
        .pipe(_rename_columns_via_glossary, year_built_glossary)
        .pipe(_melt_year_built_columns)
        .pipe(_clean_year_built_columns)
        .pipe(_extract_dublin_small_areas, sa_geometries)
        .pipe(_link_dublin_small_areas_to_geometries, sa_geometries)
        .pipe(_link_small_areas_to_postcodes, postcodes)
    )
