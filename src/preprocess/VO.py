import os
from pathlib import Path
from shutil import rmtree

import geopandas as gpd  # to read/write spatial data
import numpy as np
import pandas as pd
from funcy import log_durations
from pipeop import pipes
from toolz.functoolz import pipe

import src
from src.helper.logging import create_logger, log_df
from src.helper.paths import DATA_DIR, PLOT_DIR, ROOT_DIR
from src.helper.geocode import geocode_df

LOGGER = create_logger(caller=__name__)
INPUT_NAME = 'VO.pkl'
OUTPUT_NAME = 'vo'


@pipes
def _load_clean_and_geolocate_vo():

    return (
        _load_vo()
        >> _clean_column_names
        >> _drop_all_empty_rows
        >> _drop_non_dublin_data_from_vo
        >> _merge_columns_into_address
        >> _make_addresses_lowercase
        >> _convert_to_geodf
        >> _convert_itm_coordinates_to_floats
        >> _create_gps_coordinates_column
        >> _create_alternative_address_column_via_geolocation
        >> _drop_columns
    )


@log_durations(LOGGER.info)
def load_clean_geocode_save() -> None:

    vo_clean = _load_clean_and_geolocate_vo()

    save_path = ROOT_DIR / 'data' / 'interim' / OUTPUT_NAME
    vo_clean.to_file(save_path)


@log_df(LOGGER, columns=20)
def _load_vo() -> pd.DataFrame:

    vo_path = ROOT_DIR / 'data' / 'interim' / f'{INPUT_NAME}'
    vo = pd.read_pickle(vo_path)

    return vo


@log_df(LOGGER)
def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:

    return (
        df
        .rename(columns=str.lower)
        .rename(columns=str.strip)
    )


@log_df(LOGGER)
def _drop_all_empty_rows(vo_raw: pd.DataFrame) -> pd.DataFrame:

    return vo_raw[vo_raw.notnull()]


@log_df(LOGGER)
def _drop_non_dublin_data_from_vo(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return vo[vo['x itm'] > 100000]


@log_df(LOGGER)
def _merge_columns_into_address(vo_raw: pd.DataFrame) -> pd.DataFrame:

    vo_raw['Address'] = (
        vo_raw[[
            'Address',
            'Uses',
            'Floor Use',
            ' Local Authority',
        ]].fillna('').astype(str).apply(' '.join, axis=1)
    )
    return vo_raw


@log_df(LOGGER)
def _make_addresses_lowercase(df: pd.DataFrame) -> pd.DataFrame:

    return df.assign(Address=df['Address'].str.lower())


@log_df(LOGGER)
def _convert_to_geodf(vo: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):

    locations = gpd.points_from_xy(x=vo['x itm'], y=vo['y itm'])
    return (
        gpd
        .GeoDataFrame(vo, geometry=locations, crs='epsg:2157')
        .to_crs('epsg:4326')
    )


@log_df(LOGGER)
def _convert_itm_coordinates_to_floats(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf[['x itm', 'y itm']] = gdf[['x itm', 'y itm']].astype(np.float32)

    return gdf


@log_df(LOGGER)
def _create_gps_coordinates_column(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    vo['coordinates'] = (
        vo.geometry.y.astype(str)
        + ', '
        + vo.geometry.x.astype(str)
    )

    return vo


@log_df(LOGGER)
def _create_alternative_address_column_via_geolocation(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return geocode_df(
        df=df,
        logger=LOGGER,
        data_name='vo',
    )


@log_df(LOGGER)
def _drop_columns(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    keep_columns = [
        'address',
        'localhost_address',
        'area',
        'property number',
        'geometry',
    ]
    LOGGER.debug(f'Drop all columns except: {keep_columns}')
    return vo[keep_columns]


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
