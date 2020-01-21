from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from toolz.functoolz import pipe
from pipeop import pipes
import geopandas as gpd
import fuzzy_pandas

import src
from src.helper.logging import create_logger, log_dataframe
from src.helper.plotting import plot_geodf
import src.helper.save as save

ROOT_DIR = Path(src.__path__[0]).parent
BER_FILENAME = 'ber_clean.pkl'
DUBLIN_POSTCODES_FILENAME = 'map_of_dublin_postcodes'
OUTPUT_FILENAME = 'ber2016_gdf'
LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)


def _load_ber() -> pd.DataFrame:

    filepath = ROOT_DIR/'data'/'interim'/BER_FILENAME
    ber = pd.read_pickle(filepath)

    log_dataframe(
        df=ber,
        logger=LOGGER,
        name='Cleaned ber loaded',
    )
    return ber


def _dissolve_postcodes(pcodes: gpd.GeoDataFrame) -> pd.DataFrame:
    ''' Merge all co. dublin pcodes into one shape'''
    return pcodes.dissolve(by='Postcodes')


def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    path_to_dublin_postcodes_map = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / DUBLIN_POSTCODES_FILENAME
        / f'{DUBLIN_POSTCODES_FILENAME}.shp'
    )
    dublin_postcodes = gpd.read_file(
        filename=path_to_dublin_postcodes_map,
    )

    log_dataframe(
        df=dublin_postcodes,
        logger=LOGGER,
        name='Cleaned postcodes loaded',
    )
    return dublin_postcodes


def _merge_ber_and_map(
    ber: pd.DataFrame,
    dublin_postcodes: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    merged = pd.merge(
        ber.reset_index(),
        dublin_postcodes.reset_index(),
    )
    merged.set_index('Postcodes')

    return gpd.GeoDataFrame(merged, crs='epsg:4326')


def _plot_dublin(dublin: gpd.GeoDataFrame) -> None:

    save_path = ROOT_DIR/'plots'/'dublin_postcodes'
    plot_geodf(
        gdf=dublin,
        save_path=save_path,
        logger=LOGGER,
        name='Plot of Dublin postcodes',
    )


@pipes
def data_pipeline() -> gpd.GeoDataFrame:

    ber = _load_ber()
    dublin_postcodes = (
        _load_dublin_postcodes()
        >> _dissolve_postcodes
    )

    return _merge_ber_and_map(ber, dublin_postcodes)


def load_merge_save() -> None:

    merged = data_pipeline()
    save_path = ROOT_DIR/'data'/'interim' / OUTPUT_FILENAME

    LOGGER.info(
        'Electoral District shapefile '
        + f'merged with ber2016 data and stored at {save_path}'
    )


if __name__ == '__main__':

    LOGGER.info('Call funcs manually to execute!')
