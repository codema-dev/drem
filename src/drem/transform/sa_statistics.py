from typing import List

import geopandas as gpd
import pandas as pd

from icontract import ensure
from icontract import require
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


@require(lambda statistics: "small_area" in statistics.columns)
@require(lambda geometries: "small_area" in geometries.columns)
def _extract_dublin_small_areas(
    statistics: pd.DataFrame, geometries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return geometries.copy().merge(statistics)


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
        small_area_statistics (gpd.GeoDataFrame): Statistics data containing small area geometries
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


@require(
    lambda ber: set(ber.columns)
    == {"postcodes", "period_built", "mean_heat_demand_per_hh"},
)
def _link_small_areas_to_ber(
    small_area_statistics: gpd.GeoDataFrame, ber: pd.DataFrame,
) -> gpd.GeoDataFrame:

    return small_area_statistics.merge(ber, on=["postcodes", "period_built"])


@task(name="Transform CSO Small Area Statistics via Glossary")
def transform_sa_statistics(
    statistics: pd.DataFrame,
    glossary: pd.DataFrame,
    sa_geometries: gpd.GeoDataFrame,
    postcodes: gpd.GeoDataFrame,
    ber: pd.DataFrame,
) -> pd.DataFrame:
    """Transform CSO Small Area Statistics.

    By:
        - Extracts year built columns via glossary
        - Melts year built columns into rows for each year built type
        - Clean year built column:
            $ remove irrelevant substrings
            $ drop irrelevant columns
            $ rename columns
            $ set dtypes
        - Extract Dublin Small Areas
        - Link Small Areas to postcodes
        - TODO: Map Period built to regulatory period
        - Link Small Areas to BER on archetypes

    Args:
        statistics (pd.DataFrame): CSO Small Area Statistics
        glossary (pd.DataFrame): CSO Small Area Statistics Glossary
        sa_geometries (gpd.GeoDataFrame): CSO Small Area Geometries
        postcodes (gpd.GeoDataFrame): Dublin Postcode Geometries
        ber (pd.DataFrame): Archetyped BER Data

    Returns:
        pd.DataFrame: Small Area Statistics in 'tidy-data' format
    """
    return (
        statistics.pipe(_extract_year_built, glossary)
        .pipe(_melt_year_built_columns)
        .pipe(_clean_year_built_columns)
        .pipe(_extract_dublin_small_areas, sa_geometries)
        .pipe(_link_small_areas_to_postcodes, postcodes)
        .pipe(_link_small_areas_to_ber, ber)
    )
