import src
from src.helper.logging import create_logger, log_dataframe
from src.helper.geocode import geocode_df_addresses
import src.helper.fuzzymatch as fuzzy
from pathlib import Path

from typing import Tuple
from toolz.functoolz import pipe
from pipeop import pipes
import geopandas as gpd  # to read/write spatial data
import pandas as pd
import recordlinkage


ROOT_DIR = Path(src.__path__[0]).parent
LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)
INPUT_NAME = 'FOI_Codema_24.1.20.xlsx'
OUTPUT_NAME = 'mnr geolocated 07-04-2020.csv'


def _load_mnr() -> pd.DataFrame:

    mnr_path = ROOT_DIR / 'data' / 'raw' / INPUT_NAME
    mnr_mprn = pd.read_excel(
        mnr_path, sheet_name='MPRN_data', engine='openpyxl'
    )
    mnr_gprn = pd.read_excel(
        mnr_path, sheet_name='GPRN_data', engine='openpyxl'
    )

    log_dataframe(mnr_mprn, LOGGER, name="mnr raw mprn")
    log_dataframe(mnr_gprn, LOGGER, name="mnr raw gprn")
    return mnr_mprn, mnr_gprn


def _filter_out_non2018_data(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    mnr_mprn, mnr_gprn = mnr[0], mnr[1]

    year = 2018
    mask_mprn = mnr_mprn['Year'] == year
    mask_gprn = mnr_gprn['Year'] == year
    return mnr_mprn[mask_mprn], mnr_gprn[mask_gprn]


def _merge_columns_into_address(mnr_raw: pd.DataFrame) -> pd.DataFrame:

    mnr_raw['Address'] = (
        mnr_raw[[
            'PB Name',
            'Location',
            'County',
        ]].fillna('').astype(str).apply(' '.join, axis=1)
    )

    log_dataframe(
        df=mnr_raw,
        logger=LOGGER,
        name='Merge columns into Address column',
    )
    return mnr_raw


def _merge_columns_into_address_mprn_and_gprn(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    mnr_mprn, mnr_gprn = mnr[0], mnr[1]

    return (
        _merge_columns_into_address(mnr_mprn),
        _merge_columns_into_address(mnr_gprn),
    )


def _merge_mprn_and_gprn(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> pd.DataFrame:

    mnr_mprn, mnr_gprn = mnr[0], mnr[1]

    return fuzzy.fuzzywuzzy_merge(
        left=mnr_mprn,
        right=mnr_gprn,
        left_on='Address',
        right_on='Address',
    )


def _remove_repeating_words_in_address_column(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    mnr['Address'] = mnr['Address'].str.extract(r'\b(\w+)\s+\1\b')

    log_dataframe(
        mnr['Address'],
        logger=LOGGER,
        name='Repeating words in address column removed'
    )


@pipes
def data_pipeline() -> pd.DataFrame:

    return (
        _load_mnr()
        >> _filter_out_non2018_data
        >> _merge_columns_into_address_mprn_and_gprn
        >> _merge_mprn_and_gprn
    )


def load_convert_save() -> None:

    mnr = data_pipeline()
    save_path = ROOT_DIR / 'data' / 'interim' / f'{OUTPUT_NAME}'
    mnr_clean.to_csv(save_path)
