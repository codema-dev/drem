from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from toolz.functoolz import pipe
import geopandas as gpd
import fuzzy_pandas

import src
from src.helper.logging import create_logger, log_dataframe, log_plot_geodf
import src.helper.save as save

ROOT_DIR = Path(src.__path__[0]).parent
CENSUS_FILENAME = 'census2016_clean.pkl'
DUBLIN_EDS_FILENAME = 'dublin_EDs'
OUTPUT_FILENAME = 'census2016_gdf'
LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)


def _load_census() -> pd.DataFrame:

    filepath = ROOT_DIR/'data'/'interim'/CENSUS_FILENAME
    census = pd.read_pickle(filepath)

    log_dataframe(
        df=census,
        logger=LOGGER,
        name='Cleaned Census2016 loaded',
    )
    return census


def _load_dublin_eds() -> gpd.GeoDataFrame:

    path_to_dublin_eds = (
        ROOT_DIR / 'data' / 'interim'
        / DUBLIN_EDS_FILENAME / f'{DUBLIN_EDS_FILENAME}.shp'
    )
    dublin_eds = gpd.read_file(filename=path_to_dublin_eds)

    log_dataframe(
        df=dublin_eds,
        logger=LOGGER,
        name='Cleaned Census2016 loaded',
    )
    return dublin_eds


def _merge_census_and_map(
    census: pd.DataFrame,
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    merged = pd.merge(
        census.reset_index(),
        dublin_eds,
    )
    merged.set_index(['EDs', 'Dwelling_Type', 'Period_Built'])

    return gpd.GeoDataFrame(merged, crs='epsg:4326')


def _plot_dublin(dublin: gpd.GeoDataFrame) -> None:

    save_path = ROOT_DIR/'plots'/'dublin_EDs'
    log_plot_geodf(
        gdf=dublin,
        save_path=save_path,
        logger=LOGGER,
        name='Plot of Dublin EDs',
    )


def data_pipeline() -> gpd.GeoDataFrame:

    census = _load_census()
    dublin_eds = _load_dublin_eds()

    return _merge_census_and_map(census, dublin_eds)


def load_merge_save() -> None:

    merged = data_pipeline()
    save_path = ROOT_DIR/'data'/'interim' / OUTPUT_FILENAME

    LOGGER.info(
        f'Electoral District shapefile merged with Census2016 data and stored at {save_path}'
    )


if __name__ == '__main__':

    LOGGER.info('Call funcs manually to execute!')
