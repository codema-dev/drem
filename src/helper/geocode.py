import src
from src.helper.logging import create_logger, log_dataframe
from src.helper.paths import DATA_DIR

from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import GoogleV3, Nominatim
from tqdm import tqdm
from pathlib import Path
import logging

import pandas as pd
import geopandas as gpd


def _geocode_addresses(
    df: pd.DataFrame,
    logger: logging.Logger,
    geocoder: str,
) -> pd.DataFrame:
    ''' Geocodes addresses to gps coordinates using geopy

        Arguments:
            df {pd.DataFrame}
            logger {logging.Logger}
            geocoder {str} -- geocoder to be used by geopy

        Raises:
            ValueError: /'geocoder/' must be localhost, osm or googlemaps

        Returns:
            pd.DataFrame -- with geolocated coordinates matching the 
            addresses
        '''

    geolocation_results = df.copy()

    if geocoder == 'localhost':
        logger.info('Using Nominatim localhost:7070')
        geolocator = (
            Nominatim(domain='localhost:8080')
        )
        geocode = geolocator.geocode

    elif geocoder == 'osm':
        logger.info('Using Nominatim')
        geolocator = (
            Nominatim(user_agent='<Codema><email=rowan.molony@codema.ie')
        )
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    elif geocoder == 'googlemaps':
        logger.info('Using Googlemaps')
        geolocator = (
            GoogleV3(api_key='AIzaSyCIKb4CGbyVUqV4djjxU90gmMGC1SHK5z8')
        )
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.2)

    else:
        raise ValueError(f'Geocoder must be either osm or googlemaps')

    tqdm.pandas()
    geolocation_results['Point'] = (
        geolocation_results['Address'].progress_apply(geocode)
    )

    log_dataframe(
        df=geolocation_results,
        logger=logger,
        name=f'Df geocoded using {geocoder}',
    )
    return geolocation_results


def _reverse_geocode_coordinates(
    coordinates: pd.Series,
    logger: logging.Logger,
    geocoder: str,
) -> pd.DataFrame:
    ''' Reverse geocodes df['Location'] column to an address using geopy

        Warning:
        Requires location column to be in the form /'<Latitude>, <Longitude>/'
        or /'53.40642945, -6.28710147367475/'

        Arguments:
            coordinates {pd.Series}
            logger {logging.Logger}
            geocoder {str} -- geocoder to be used by geopy


        Raises:
            ValueError: /'geocoder/' must be localhost, osm or googlemaps

        Returns:
            pd.DataFrame -- with geolocated address column
        '''

    geolocation_results = pd.DataFrame(
        data=coordinates,
        columns='coordinates',
    )

    if geocoder == 'localhost':
        logger.info('Using Nominatim localhost:7070')
        geolocator = (
            Nominatim(domain='localhost:7070', scheme='http', timeout=60)
        )
        rgeocode = geolocator.reverse

    elif geocoder == 'osm':
        logger.info('Using Nominatim')
        geolocator = (
            Nominatim(user_agent='<Codema><email=rowan.molony@codema.ie')
        )
        rgeocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    elif geocoder == 'googlemaps':
        logger.info('Using Googlemaps')
        geolocator = (
            GoogleV3(api_key='AIzaSyCIKb4CGbyVUqV4djjxU90gmMGC1SHK5z8')
        )
        rgeocode = RateLimiter(geolocator.reverse, min_delay_seconds=0.2)

    else:
        raise ValueError(f'Geocoder must be localhost, osm or googlemaps')

    tqdm.pandas()
    geolocation_results[f'{geocoder}_address'] = (
        geolocation_results['coordinates'].progress_apply(rgeocode)
    )

    log_dataframe(
        df=geolocation_results,
        logger=logger,
        name=f'Df geocoded using {geocoder}',
    )
    return geolocation_results


def geocode_df(
    df: pd.DataFrame,
    logger: logging.Logger,
    data_name: str,
    geocoder: str = 'localhost',
) -> gpd.GeoDataFrame:

    geocode_filepath = (
        DATA_DIR
        / 'interim'
        / f'{data_name}_{geocoder}_geocode_results.csv'
    )

    if data_name == 'mnr':

        if geocode_filepath.exists():
            geolocation_results = pd.read_csv(geocode_filepath)

        else:
            geolocation_results = _geocode_addresses(
                df=df[['ID', 'Address']],
                logger=logger,
                geocoder=geocoder,
            )
            geolocation_results.to_csv(geocode_filepath, index=False)

        geocoded_df = df.merge(
            geolocation_results[['ID', 'Point']],
            on='ID',
        )

    elif data_name == 'vo':

        if geocode_filepath.exists():
            geolocation_results = pd.read_csv(geocode_filepath)

        else:
            geolocation_results = _reverse_geocode_coordinates(
                coordinates=df['coordinates'],
                logger=logger,
                geocoder=geocoder,
            )
            geolocation_results.to_csv(geocode_filepath, index=False)

        geocoded_df = df.merge(geolocation_results, on='coordinates')

    else:
        raise ValueError('data_name must be mnr or vo')

    log_dataframe(
        df=geocoded_df,
        logger=logger,
        name=f'Df geocoded using {geocoder}',
    )

    return geocoded_df
