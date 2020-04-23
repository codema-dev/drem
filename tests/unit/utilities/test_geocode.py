from codema_drem.utilities.geocode import (
    _setup_geolocator,
    _setup_geocoder,
    geocode_coordinates,
    geocode_addresses,
)
from geopy import Nominatim, GoogleV3
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from contextlib import contextmanager


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail(f"Raised {exception}")


@pytest.fixture(autouse=True)
def nominatim_reverse_geocoder() -> Nominatim:
    geocoding_platform = 'localhost'
    geolocator = _setup_geolocator(geocoding_platform)
    return _setup_geocoder(
        geocoding_platform=geocoding_platform,
        geolocator=geolocator,
        reverse=True,
    )


@pytest.fixture(autouse=True)
def nominatim_geocoder() -> Nominatim:
    geocoding_platform = 'localhost'
    geolocator = _setup_geolocator(geocoding_platform)
    return _setup_geocoder(
        geocoding_platform=geocoding_platform,
        geolocator=geolocator,
    )


def test_localhost_server_connection_exists():

    from geopy.exc import GeocoderServiceError
    geolocator = Nominatim(domain='localhost:7070', scheme='http')
    geocoder = geolocator.geocode
    try:
        geocoder('The Spire, Dublin')
    except GeocoderServiceError:
        pytest.fail(
            'Connection to Geocoding platform server failed!\n'
            f'Geocoding platform: {geocoding_platform}\n'
            '\t If geocoding_platform is localhost then need to setup\n'
            '\t server locally via Docker, for more details see:\n'
            '\t https://github.com/mediagis/nominatim-docker\n'
        )


def test_geolocator_setup_instances():

    assert isinstance(_setup_geolocator('localhost'), Nominatim)
    assert isinstance(_setup_geolocator('osm'), Nominatim)
    assert isinstance(_setup_geolocator('googlemaps'), GoogleV3)


def test_geolocator_bad_input():

    with pytest.raises(ValueError):
        _setup_geolocator('blah')


def test_geocoder_coordinates(nominatim_reverse_geocoder):

    geocoder = nominatim_reverse_geocoder
    location = geocoder('53.3524574, -6.261041')
    assert 'Blu Apple' in location.address
    # ... depressingly osm doesn't guess spire


def test_geocoder_addresses(nominatim_geocoder):

    geocoder = nominatim_geocoder
    location = geocoder('The Spire, Dublin')
    assert location.latitude == 53.3524573
    assert location.longitude == -6.261041


def test_geocoder_coordinates_series(nominatim_reverse_geocoder):

    geocoder = nominatim_reverse_geocoder
    coordinates_series = pd.Series([
        '53.3524574, -6.261041',
        '53.2894484, -6.11386775',
    ])

    locations = coordinates_series.apply(geocoder)
    assert 'Blu Apple' in locations.iloc[0].address
    assert 'Forty Foot' in locations.iloc[1].address


def test_geocoder_addresses_series(nominatim_geocoder):

    geocoder = nominatim_geocoder
    coordinates_series = pd.Series([
        'The Spire, Dublin',
        'Forty Foot, Dublin',
    ])

    locations = coordinates_series.apply(geocoder)
    assert locations.iloc[0].latitude == 53.3524573
    assert locations.iloc[0].longitude == -6.261041
    assert locations.iloc[1].latitude == 53.2894484
    assert locations.iloc[1].longitude == -6.11386775


def test_geocode_coordinates_bad_input(tmp_path):

    bad_input_series = pd.Series([
        '53.3524573, 6.261041',
        '53.2894484, -6.11386775',
    ])  # +ve longitude
    with pytest.raises(ValueError):
        geocode_coordinates(
            coordinates=bad_input_series,
            save_path=tmp_path / 'bad_input_rgeocode',
        )


def test_geocode_coordinates_bad_input_none(tmp_path):

    bad_input_series = pd.Series([
        '53.3524573, -6.261041',
        None, None,
        '53.2894484, -6.11386775',
    ])  # +ve longitude
    with pytest.raises(ValueError):
        geocode_coordinates(
            coordinates=bad_input_series,
            save_path=tmp_path / 'bad_input_rgeocode',
        )


def test_geocode_coordinates_good_input(tmp_path):

    good_input_series = pd.Series([
        '53.3524573, -6.261041',
        '53.2894484, -6.11386775',
    ])

    save_path = tmp_path / 'geocode_coordinates'

    geocode_coordinates(
        coordinates=good_input_series,
        save_path=save_path,
    )
    geocode_result = pd.read_csv(save_path, usecols=['address'])
    assert 'Blu Apple' in geocode_result.iloc[0].to_string()
    assert 'Forty Foot' in geocode_result.iloc[1].to_string()


#
# def test_geocode_addresses_bad_input(tmp_path):
#
#    bad_input_series = pd.Series([
#        'The Spire, Dublin',
#        'Forty Foot, Dublin',
#    ])  # +ve longitude
#    with pytest.raises(ValueError):
#        geocode_addresses(
#            addresses=bad_input_series,
#            save_path=tmp_path / 'bad_input_geocode')
#


def test_geocode_addresses_good_input(tmp_path):

    good_input_series = pd.Series([
        'The Spire, Dublin',
        'Forty Foot, Dublin',
    ])
    expected_result = pd.DataFrame([
        [53.3524573, -6.261041],
        [53.2894484, -6.11386775],
    ], columns=['latitude', 'longitude'])

    save_path = tmp_path / 'geocode_addresses'

    geocode_addresses(
        addresses=good_input_series,
        save_path=save_path,
    )
    geocode_result = pd.read_csv(
        save_path,
        usecols=['latitude', 'longitude'],
    )
    assert_frame_equal(expected_result, geocode_result)
