import pysnooper
from pathlib import Path
from typing import Union

import pandas as pd
from geopy.exc import GeocoderServiceError
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import GoogleV3, Nominatim
from icontract import require
from tqdm import tqdm


@require(lambda geocoding_platform:
         geocoding_platform in ('localhost', 'osm', 'googlemaps'),
         error=lambda geocoding_platform:
         ValueError(
             f'Geocoding platform \'{geocoding_platform}\' not implemented,'
             r" must be one of ('localhost', 'osm', 'googlemaps')"))
def _setup_geolocator(
    geocoding_platform: str,
) -> Union[Nominatim, GoogleV3]:
    """Initialise a Geopy geocoding object to link to a Geocoding server

    Parameters
    ----------
    geocoding_platform : str, optional
        Options: localhost, osm or googlemaps, by default 'localhost'

    Returns
    -------
    Union[Nominatim, GoogleV3]
        A Geopy geolocation object which provides a geocoding server API
        (i.e. enables querying addresses or coordinates)
    """

    email = 'rowan.molony@codema.ie'
    gmaps_api_key = '<GOOGLE MAPS API KET>'

    geocoders = {
        'localhost': Nominatim(domain='localhost:7070', scheme='http'),
        'osm': Nominatim(user_agent=f'<Codema><email={email}'),
        'googlemaps': GoogleV3(api_key=gmaps_api_key),
    }

    return geocoders[geocoding_platform]


def _setup_geocoder(
    geocoding_platform: str,
    geolocator: Union[Nominatim, GoogleV3],
    reverse: bool = False,
) -> Union[Nominatim, GoogleV3]:
    """Setup a geocode or (optionally) a reverse geocode Geopy object

    Parameters
    ----------
    geocoding_platform : str, optional
        Options: localhost, osm or googlemaps
    reverse : bool, optional
        Optionally query coordinates, by default False

    Returns
    -------
    Union[Nominatim, GoogleV3]
        A Geopy geocoder which can be applied to addresses (or coordinates)
        to find matching coordinates (or addresses)
    """

    if reverse:
        geocoders = {
            'localhost': geolocator.reverse,
            'osm': RateLimiter(geolocator.reverse, min_delay_seconds=1),
            'googlemaps': RateLimiter(geolocator.reverse, min_delay_seconds=1),
        }

    else:
        geocoders = {
            'localhost': geolocator.geocode,
            'osm': RateLimiter(geolocator.geocode, min_delay_seconds=1),
            'googlemaps': RateLimiter(geolocator.geocode, min_delay_seconds=1),
        }

    return geocoders[geocoding_platform]


def _geocode(
    input_data: pd.Series,
    geolocator: Union[Nominatim, GoogleV3],
    geocoder: Union[Nominatim, GoogleV3],
    geocoding_platform: str = 'localhost',
) -> pd.Series:

    tqdm.pandas()
    try:
        geocoded_data = input_data.progress_apply(geocoder)

    except GeocoderServiceError:
        raise Exception(
            'Connection to Geocoding platform server failed!\n'
            f'Geocoding platform: {geocoding_platform}\n'
            '\t If geocoding_platform is localhost then need to setup\n'
            '\t server locally via Docker, for more details see:\n'
            '\t https://github.com/mediagis/nominatim-docker\n'
        )

    return geocoded_data


def geocode_addresses(
    addresses: pd.Series,
    save_path: Path,
    geocoding_platform: str = 'localhost',
) -> None:
    """Geocodes a series/column of addresses using
    localhost (i.e. openstreetmaps setup locally via docker),
    osm (i.e. openstreetmaps online server) or googlemaps

    Parameters
    ----------
    geocoding_platform : str
        Options: localhost, osm or googlemaps
    addresses : pd.Series
        Series/column of data to be geocoded.
    save_path : Path
        File path to save destination on-disk


    Raises
    ------
    Exception
        [description]
    """
    geolocator = _setup_geolocator(geocoding_platform)
    rgeocoder = _setup_geocoder(
        geocoding_platform=geocoding_platform,
        geolocator=geolocator,
    )

    coordinates = pd.DataFrame()
    coordinates['geocoder_result'] = _geocode(
        input_data=addresses,
        geolocator=geolocator,
        geocoder=rgeocoder,
        geocoding_platform=geocoding_platform,
    )

    coordinates['latitude'] = (
        coordinates['geocoder_result']
        .apply(
            lambda extract: extract.latitude if extract is not None else None
        )
    )
    coordinates['longitude'] = (
        coordinates['geocoder_result']
        .apply(
            lambda extract: extract.longitude if extract is not None else None
        )
    )
    coordinates['input_address'] = addresses

    coordinates['raw'] = (
        coordinates['geocoder_result']
        .apply(
            lambda extract: extract.raw if extract is not None else None
        )
    )

    coordinates.to_csv(save_path, index=False)


# @require(lambda coordinates:
#          coordinates.str.match(r'\d{2}\.\d+, \-\d\.\d+').all(),
#          error=lambda coordinates:
#          ValueError(
#              f'{coordinates}\n'
#              'Row not in required format for geopy:'
#              ' tuple in form 53.3524574, -6.261041'))
def geocode_coordinates(
    coordinates: pd.Series,
    save_path: Path,
    geocoding_platform: str = 'localhost',
) -> None:
    """Geocodes a series/column of coordinates using
    localhost (i.e. openstreetmaps setup locally via docker),
    osm (i.e. openstreetmaps online server) or googlemaps

    Parameters
    ----------
    geocoding_platform : str
        Options: localhost, osm or googlemaps
    coordinates : pd.Series
        Series/column of latitudes
    save_path : Path
        File path to save destination on-disk

    Raises
    ------
    Exception
        [description]
    """

    geolocator = _setup_geolocator(geocoding_platform)
    geocoder = _setup_geocoder(
        geocoding_platform=geocoding_platform,
        geolocator=geolocator,
        reverse=True,
    )

    addresses = pd.DataFrame()
    addresses['geocoder_result'] = _geocode(
        input_data=coordinates,
        geolocator=geolocator,
        geocoder=geocoder,
        geocoding_platform=geocoding_platform,
    )

    addresses['address'] = (
        addresses['geocoder_result']
        .apply(
            lambda extract: extract.address if extract is not None else None
        )
    )

    addresses['input_coordinates'] = coordinates

    addresses['raw'] = (
        addresses['geocoder_result']
        .apply(
            lambda extract: extract.raw if extract is not None else None
        )
    )

    addresses.to_csv(save_path, index=False)
