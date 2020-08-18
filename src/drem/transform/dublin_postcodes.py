#!/usr/bin/env python

from re import VERBOSE

import geopandas as gpd

from icontract import require
from prefect import task


@require(lambda postcodes: "postcodes" in postcodes.columns)
def _rename_postcodes(postcodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    pattern = """
    ^               # at start of string
    ([^Dublin])     # doesn't contain 'Dublin' substring
    """
    mask = postcodes["postcodes"].str.contains(pat=pattern, flags=VERBOSE)
    postcodes.loc[mask, "postcodes"] = "Co. Dublin"

    return postcodes


@task(name="Transform Dublin Postcodes")
def transform_dublin_postcodes(postcodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Tidy Dublin Postcodes.

    - Rename column to postcodes
    - Replace all non Dublin <number> postcodes with Co. Dublin

    Args:
        postcodes (gpd.GeoDataFrame): [description]

    Returns:
        gpd.GeoDataFrame: [description]
    """
    return postcodes.rename(columns={"Yelp_postc": "postcodes"}).pipe(_rename_postcodes)
