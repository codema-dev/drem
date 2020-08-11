from typing import List

import geopandas as gpd
import pandas as pd

from prefect import task


def _extract_year_built(data: pd.DataFrame, glossary: pd.DataFrame) -> pd.DataFrame:

    index_of_year_built_row = glossary.query(
        "`Tables Within Themes` == 'Permanent private households by year built '",
    ).index.item()
    # Need to subtract 1 as the relevant rows start one row above the text used to search
    start_row: int = index_of_year_built_row - 1

    index_of_occupancy_row = glossary.query(
        "`Tables Within Themes` == 'Permanent private households by type of occupancy '",
    ).index.item()
    end_row: int = index_of_occupancy_row - 1

    year_built_glossary: pd.DataFrame = (
        glossary.iloc[start_row:end_row][["Column Names", "Description of Field"]]
        .set_index("Column Names")
        .to_dict()["Description of Field"]
    )

    return (
        data.copy()
        .loc[:, ["GEOGID"] + list(year_built_glossary.keys())]
        .rename(columns=year_built_glossary)
    )


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


def _extract_dublin_small_areas(
    statistics: pd.DataFrame, geometries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return geometries.merge(statistics)


@task(name="Transform CSO Small Area Statistics via Glossary")
def transform_sa_statistics(
    statistics: pd.DataFrame, glossary: pd.DataFrame, geometries: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Transform CSO Small Area Statistics via Glossary to 'tidy-data'.

    Args:
        statistics (pd.DataFrame): CSO Small Area Statistics
        glossary (pd.DataFrame): CSO Small Area Statistics Glossary
        geometries (gpd.GeoDataFrame): CSO Small Area Geometries

    Returns:
        pd.DataFrame: Small Area Statistics in 'tidy-data' format
    """
    return (
        statistics.pipe(_extract_year_built, glossary)
        .pipe(_melt_year_built_columns)
        .pipe(_clean_year_built_columns)
        .pipe(_extract_dublin_small_areas, geometries)
    )
