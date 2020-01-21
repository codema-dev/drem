import time
from sklearn.feature_extraction.text import TfidfVectorizer
import sparse_dot_topn.sparse_dot_topn as ct
from scipy.sparse import csr_matrix
from ftfy import fix_text  # amazing text cleaning for decode issues..
from pathlib import Path
from typing import List

import geopandas as gpd  # to read/write spatial data
import matplotlib.pyplot as plt  # to visualize data
import numpy as np
import pandas as pd
# import pyjanitor

from dpcontracts import require
from funcy import log_durations
from fuzzywuzzy import fuzz, process
from toolz.functoolz import pipe
from pipeop import pipes
import pandas_dedupe
from shapely.wkt import loads

import src
from scipy.spatial import cKDTree
from src.helper import save
from src.helper.logging import create_logger, log_dataframe
import src.helper.fuzzymatch as fuzzy

import re


from src.helper.paths import ROOT_DIR, DATA_DIR, PLOT_DIR
LOGGER = create_logger(caller=__name__)
VO_FILENAME = 'vo geolocated 8-04-2020.csv'


def _load_vo() -> gpd.GeoDataFrame:

    vo_path = (
        ROOT_DIR / 'data' / 'interim' / VO_FILENAME
    )

    vo = pd.read_csv(vo_path, index_col=0)

    # ! workaround: convert geometry column to type shapely
    vo['geometry'] = vo['geometry'].apply(loads)
    vo_geo = gpd.GeoDataFrame(vo, crs='epsg:4326', geometry='geometry')

    log_dataframe(vo_geo, LOGGER, name='Map of Dublin')
    return vo_geo


def _load_mnr() -> gpd.GeoDataFrame:

    mnr_path = ROOT_DIR / 'data' / 'interim' / 'mnr' / 'mnr.shp'
    mnr = gpd.read_file(mnr_path)

    log_dataframe(mnr, LOGGER, name='M&R')
    return mnr


def _load_map_of_dublin() -> gpd.GeoDataFrame:

    map_of_dublin_path = (
        ROOT_DIR / 'data' / 'interim' /
        'map_of_dublin_EDs' / 'map_of_dublin_EDs.shp'
    )
    map_of_dublin = gpd.read_file(map_of_dublin_path)

    log_dataframe(map_of_dublin, LOGGER, name='VO')
    return map_of_dublin


def load_data() -> (gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame):

    vo, mnr, map_of_dublin = (
        _load_vo(),
        _load_mnr(),
        _load_map_of_dublin()
    )

    LOGGER.info('Data Loaded!')

    return vo, mnr, map_of_dublin


def _add_buffer_to_mnr(
    mnr: gpd.GeoDataFrame,
    distance: float = 0.005,
) -> gpd.GeoDataFrame:

    searchbuffer = mnr.buffer(distance)
    return gpd.GeoDataFrame(
        mnr.drop(columns='geometry'),
        geometry=searchbuffer.to_list(),
        crs='epsg:4326',
    )


def _spatial_join_nearby_buildings(
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    mnr_buffered = _add_buffer_to_mnr(mnr)

    merged = gpd.sjoin(
        left_df=mnr_buffered,
        right_df=vo,
        op='intersects',
        how='left',
    )

    merged.sort_values(by='Address_left')
    log_dataframe(merged, LOGGER, name='VO and buffered M/&R merged')
    return merged


@log_durations(LOGGER.info)
def _fuzzywuzzy_fjoin_buildings_in_buffer(
    sjoined_vo_mnr: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return fuzzy.fuzzywuzzy_best_column_matches(
        gdf=sjoined_vo_mnr,
        col1='Address_left',
        col2='Location_right',
    )


@pipes
def _sjoin_and_fjoin_vo_and_mnr(
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return (
        _spatial_join_nearby_buildings(vo, mnr)
        >> _fuzzywuzzy_fjoin_buildings_in_buffer
    )


@log_durations(LOGGER.info)
def pure_fuzzywuzzy_match_and_add_vo_address_to_mnr(
    mnr: gpd.GeoDataFrame,
    vo: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    mnr_with_vo_addresses = fuzzy.fuzzywuzzy_ljoin_two_dataframes(
        df_left=mnr,
        df_right=vo,
        left_on='Address',
        right_on='Address',
    )

    log_dataframe(
        df=mnr_with_vo_addresses,
        logger=LOGGER,
        name='VO addresses matched and added to MnR',
    )
    return mnr_with_vo_addresses


def pandas_dedupe_fuzzy_linkage(
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    dfs_linked = pandas_dedupe.link_dataframes(
        mnr,
        vo,
        field_properties=['Address', 'Address']
    )

    log_dataframe(
        dfs_linked,
        LOGGER,
        name='Dfs linked using pandas_dedupe',
    )
    return dfs_linked


def _plot_fuzzymatch_results(
    vo_mnr_fuzzymatched: pd.DataFrame,
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame,
    map_of_dublin: gpd.GeoDataFrame,
) -> None:

    fig, ax = plt.subplots(sharex=True, sharey=True, figsize=(20, 16))
    plot_path = PLOT_DIR / 'fuzzymatch_vo_mnr_Rebecca_data.png'

    vo_mask = (
        vo['Address'].isin(vo_mnr_fuzzymatched['Closest Address'])
    )

    map_of_dublin.plot(ax=ax, color='white', edgecolor='black')
    mnr.buffer(0.005).plot(ax=ax, color='yellow')
    mnr.plot(ax=ax, color='blue', markersize=0.2)
    vo[vo_mask].plot(ax=ax, color='red', markersize=0.2)

    fig.savefig(plot_path)
    LOGGER.info('VO & M&R fuzzymatch plotted.')


def _ckdnearest(
    mnr: gpd.GeoDataFrame,
    vo: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    nA = np.array(list(zip(mnr.geometry.x, mnr.geometry.y)))
    nB = np.array(list(zip(vo.geometry.x, vo.geometry.y)))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)

    return vo.loc[idx]


def _plot_vo_mnr_dublin(
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame,
    map_of_dublin: gpd.GeoDataFrame,
) -> None:

    fig, ax = plt.subplots(sharex=True, sharey=True, figsize=(20, 16))
    plot_path = PLOT_DIR / 'vo_mnr_dublin.png'

    map_of_dublin.plot(ax=ax, color='white', edgecolor='black')
    vo.plot(ax=ax, color='red', markersize=0.1)
    # mnr.plot(ax=ax, color='blue', markersize=0.1)
    mnr.plot(ax=ax, color='blue')

    fig.savefig(plot_path)


def _save_geodataframe_to_shapefile(gdf: gpd.GeoDataFrame, name: str):

    save_path = DATA_DIR / name / f'{name}.shp'
    save.geodataframe(gdf, save_path)


if __name__ == '__main__':

    LOGGER.info('Call funcs manually to execute!')
