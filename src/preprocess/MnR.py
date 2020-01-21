import os
from pathlib import Path
from shutil import rmtree

import geopandas as gpd  # to read/write spatial data
import numpy as np
import pandas as pd
from pipeop import pipes
from toolz.functoolz import pipe
from typing import Tuple

import src
from src.data import load, path
from src.helper.geocode import geocode_df
from src.helper.logging import create_logger, log_dataframe, log_df
from src.helper.paths import DATA_DIR, PLOT_DIR, ROOT_DIR

LOGGER = create_logger(caller=__name__)
INPUT_NAME_REBECCA = '(Rebecca) Updated Public Sector M&R.xlsx'
INPUT_NAME_RAW = 'FOI_Codema_24.1.20.xlsx'
OUTPUT_NAME = 'mnr'


@   pipes
def load_clean_and_geocode_mnr_rebecca() -> gpd.GeoDataFrame:

    mnr_clean = (
        _load_mnr_rebecca()
        >> _make_all_cols_lowercase
        >> _merge_columns_into_address
        >> _delete_commas_in_address_col
        >> _remove_all_duplicate_words_in_address_col
        >> _geocode_address_column
        >> _convert_geocode_string_results_to_lat_long
        >> _convert_to_geodf
        >> _drop_duplicate_rows
        >> _drop_null_locations
        >> _manually_fix_outliers_rebecca
        >> _drop_null_locations
    )

    LOGGER.info(f'M&R data cleaned and geolocated\n')
    return mnr_clean


@pipes
def load_clean_and_geocode_mnr_raw() -> gpd.GeoDataFrame:

    mnr_clean = (
        _load_mnr_raw()
        >> _filter_out_non2018_data
        >> _merge_mprn_and_gprn
    )

    LOGGER.info(f'M&R data cleaned!\n')
    return mnr_clean


def load_convert_save() -> None:

    mnr_raw = _load_mnr_rebecca()
    mnr_clean = _clean_and_geocode_mnr_rebecca(mnr_raw)

    save_path = DATA_DIR / 'interim' / f'{OUTPUT_NAME}'
    mnr_clean.to_file(save_path)


@log_df(LOGGER, columns=10)
def _load_mnr_rebecca() -> pd.DataFrame:

    path = DATA_DIR / 'interim' / f'{INPUT_NAME_REBECCA}'
    return pd.read_excel(path, sheet_name='Total Energy', engine='openpyxl')

# ! ------------------------------------------------
# ! On hold until fuzzy merging algorithm written:


def _load_mnr_raw() -> pd.DataFrame:

    mnr_path = ROOT_DIR / 'data' / 'raw' / INPUT_NAME_RAW
    mnr = pd.read_excel(
        mnr_path, sheet_name=['MPRN_data', 'GPRN_data'], engine='openpyxl'
    )
    mnr_mprn, mnr_gprn = mnr['MPRN_data'], mnr['GPRN_data']

    log_dataframe(mnr_mprn, LOGGER, name="mnr raw mprn")
    log_dataframe(mnr_gprn, LOGGER, name="mnr raw gprn")
    return (mnr_mprn, mnr_gprn)


def _filter_out_non2018_data(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    mnr_mprn = mnr[0]
    mnr_gprn = mnr[1]

    year = 2018
    mask_mprn = mnr_mprn['Year'] == year
    mask_gprn = mnr_gprn['Year'] == year
    return (mnr_mprn[mask_mprn], mnr_gprn[mask_gprn])


@log_df(LOGGER)
def _merge_mprn_and_gprn(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> pd.DataFrame:

    raise NotImplementedError('Requires fuzzy merge...')

    mnr_mprn = mnr[0]
    mnr_gprn = mnr[1]

    return pd.merge(
        left=mnr_mprn,
        right=mnr_gprn,
        left_on=['PB Name', 'Location', 'Category', 'County'],
        right_on=['PB Name', 'Location', 'Category', 'County'],
    )


# ! ------------------------------------------------


def _make_all_cols_lowercase(df: pd.DataFrame) -> pd.DataFrame:

    mask_strings = df.dtypes == np.object
    df.loc[:, mask_strings] = (
        df.loc[:, mask_strings]
        .stack()
        .str.lower()
        .unstack()
    )

    return df


@log_df(LOGGER)
def _remove_postcodes_from_location_col(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    return mnr.assign(
        Location=(
            mnr['Location']
            .str.replace(
                pat=r'\b(dublin.+)\b',
                repl='',
                regex=True,
            )
        )
    )


@log_df(LOGGER)
def _merge_columns_into_address(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    mnr['Address'] = (
        mnr[[
            'PB Name',
            'Location',
            'County',
        ]].fillna('').astype(str).apply(' '.join, axis=1)
    )

    return mnr


@log_df(LOGGER)
def _delete_commas_in_address_col(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    return mnr.assign(
        Address=mnr['Address'].str.replace(',', '')
    )


@log_df(LOGGER)
def _remove_all_duplicate_words_in_address_col(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    return mnr.assign(
        Address=(
            mnr['Address']
            .str.replace(
                pat=r'\b(\w+)\b(?=.*?\b\1\b)',
                repl='',
                regex=True,
            )
        )
    )


def _geocode_address_column(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    return geocode_df(
        df=mnr,
        logger=LOGGER,
        data_name='mnr',
        # geocoder='googlemaps',
        geocoder='localhost',
    )


def _convert_geocode_string_results_to_lat_long(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    return (
        mnr
        .assign(
            Latitude=(
                mnr['Point']
                .str.extract(r'(\d+.\d+)')
                .fillna(np.nan)
                .astype(float)
            ),
            Longitude=(
                mnr['Point']
                .str.extract(r'(-\d+.\d+)')
                .fillna(np.nan)
                .astype(float)
            ),
        )
    )


@log_df(LOGGER)
def _convert_to_geodf(mnr: pd.DataFrame) -> (gpd.GeoDataFrame):

    locations = gpd.points_from_xy(x=mnr['Latitude'], y=mnr['Longitude'])
    return gpd.GeoDataFrame(mnr, geometry=locations, crs='epsg:4326')


@log_df(LOGGER)
def _drop_duplicate_rows(mnr_raw: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    mnr_clean = mnr_raw.drop_duplicates()
    return mnr_clean


@log_df(LOGGER)
def _manually_fix_outliers_rebecca(mnr: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    mask = (mnr['Latitude'] // 53 == 0) | (mnr['Longitude'].abs() // 6 == 1)

    mnr.loc[
        mnr['PB Name'] == 'AHEAD',
        ('Latitude', 'Longitude'),
    ] = 53.2955759, -6.1878618

    mnr.loc[
        mnr['PB Name'] == 'daa plc',
        ('Latitude', 'Longitude'),
    ] = 53.42931949, -6.2462264

    mnr.loc[
        mnr['PB Name'] == 'HSE',
        ('Latitude', 'Longitude'),
    ] = np.nan, np.nan

    mnr.loc[
        mnr['PB Name'] == 'Irish Greyhound Board / Bord na gCon',
        ('Latitude', 'Longitude'),
    ] = 53.3240619, -6.277817

    mnr.loc[
        mnr['PB Name'] == 'CLOCHAR SAN DOMINIC',
        ('Latitude', 'Longitude'),
    ] = 53.2859253, -6.3572716

    # Following row is in Meath so drop it:
    mask = mnr['PB Name'] == 'National Council for Special Education'
    mnr.drop(mnr.loc[mask].index, inplace=True)

    return mnr


@log_df(LOGGER)
def _drop_null_locations(mnr_raw: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    mask = (mnr_raw['Latitude'].isnull()) | (mnr_raw['Longitude'].isnull())
    mnr_clean = mnr_raw[~mask]
    return mnr_clean
