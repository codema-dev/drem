import pandas as pd
import geopandas as gpd

import src

ROOT_DIR = Path(src.__path__[0]).parent


def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / 'map_of_dublin_postcodes'
        / 'map_of_dublin_postcodes.shp'
    )
    return gpd.read_file(filename=path)


def _load_dublin_eds() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / 'map_of_dublin_EDs'
        / 'map_of_dublin_EDs.shp'
    )
    return gpd.read_file(filename=path)


def postcode(df: pd.DataFrame) -> gpd.GeoDataFrame:

    postcodes = _load_dublin_postcodes()
    merged = pd.merge(df, postcodes)

    return gpd.GeoDataFrame(merged, crs='epsg:4326')


def electoral_district(df: pd.DataFrame) -> gpd.GeoDataFrame:

    eds = _load_dublin_eds()
    merged = pd.merge(df, eds)

    return gpd.GeoDataFrame(merged, crs='epsg:4326')
