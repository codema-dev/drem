from collections import defaultdict
from time import sleep
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import GoogleV3, Nominatim
from tqdm import tqdm
from pathlib import Path

import src
from src.helper.logging import create_logger, log_dataframe
from src.helper.geocode import reverse_geocode_df_coordinates
import src.helper.save as save
from datetime import datetime

from toolz.functoolz import pipe
import geopandas as gpd  # to read/write spatial data
import pandas as pd


ROOT_DIR = Path(src.__path__[0]).parent
LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)
INPUT_NAME = 'vo'
OUTPUT_NAME = f'vo geolocated {datetime.now().date()}.csv'


def _load_vo() -> gpd.GeoDataFrame:

    vo_path = ROOT_DIR / 'data' / 'interim' / INPUT_NAME / f'{INPUT_NAME}.shp'
    vo = gpd.read_file(vo_path)

    log_dataframe(vo, LOGGER, name="vo clean")
    return vo


def _create_location_column(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    vo['location'] = (
        vo.geometry.y.astype(str)
        + ', '
        + vo.geometry.x.astype(str)
    )

    return vo


def _rgeocode_vo(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return reverse_geocode_df_coordinates(vo)


def data_pipeline() -> gpd.GeoDataFrame:

    return pipe(
        _load_vo(),
        _create_location_column,
        _rgeocode_vo,
    )


def load_rgeocode_save() -> None:

    vo_geocoded = data_pipeline()

    save_path = ROOT_DIR / 'data' / 'interim' / f'{OUTPUT_NAME}.csv'
    vo_geocoded.to_csv(save_path)

    # save_path = (
    #     ROOT_DIR / 'data' / 'interim' / OUTPUT_NAME / f'{OUTPUT_NAME}.shp'
    # )
    # save.geodataframe(save_path, vo_geocoded)

    LOGGER.info(f'VO reverse geocoded and saved to {save_path}')
