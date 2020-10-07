from pathlib import Path
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
def transform_dublin_postcodes(dirpath: Path, filename: str) -> gpd.GeoDataFrame:
    """Transform Dublin Postcodes.

    Args:
        dirpath (Path): Directory where data is stored
        filename (str): Name of data

    Returns:
        gpd.GeoDataFrame: Clean postcodes data
    """
    filepath = dirpath / f"{filename}.parquet"
    return (
        gpd.read_parquet(filepath)
        .to_crs("epsg:4326")
        .rename(columns={"Yelp_postc": "postcodes"})
        .pipe(_rename_postcodes)
    )
