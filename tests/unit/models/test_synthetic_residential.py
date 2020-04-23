from io import StringIO

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from shapely.geometry import Point

import codema_drem
from codema_drem.models.synthetic_residential import (
    _add_column_alldublin_archetype_id,
    _add_column_postcode_archetype_id,
    _expand_census_to_individual_buildings,
    _extract_alldublin_archetype_totals,
    _extract_postcode_archetype_totals,
    _flag_small_archetype_sample_sizes,
    _link_census_to_postcodes,
    _link_to_ber_alldublin_archetype_totals,
    _link_to_ber_postcode_archetype_totals,
    _select_a_random_matching_postcode_archetype,
    _link_to_matching_postcode_archetypes,
    _select_a_random_matching_alldublin_archetype,
    _link_to_matching_alldublin_archetypes,
)


def test_link_census_to_postcodes() -> None:

    census = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            arran quay a, apartments, 1919 - 1945, 2"""
        ),
        skipinitialspace=True,
    )

    eds = gpd.GeoDataFrame(
        {"eds": ["arran quay a"], "postcodes": ["dublin 7"], "geometry": [Point(0, 0)]}
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh, postcodes
            arran quay a, apartments, 1919 - 1945, 2, dublin 7"""
        ),
        skipinitialspace=True,
    )

    output = _link_census_to_postcodes.run(census, eds[["eds", "postcodes"]])
    assert_frame_equal(output, expected_output)


def test_expand_census_to_individual_buildings() -> None:

    census = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            arran quay a, apartments, 1919 - 1945, 2"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built
            arran quay a, apartments, 1919 - 1945
            arran quay a, apartments, 1919 - 1945"""
        ),
        skipinitialspace=True,
    )

    output = _expand_census_to_individual_buildings.run(census)
    assert_frame_equal(output, expected_output)


@pytest.fixture()
def ber_with_irrelevant_col():
    return pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, irrelevant_col 
            dublin 7, apartments, 1919 - 1945, X
            dublin 17, terraced house, 1946 - 1960, X
            dublin 17, terraced house, 1946 - 1960, X
            dublin 12, terraced house, 1946 - 1960, X"""
        ),
        skipinitialspace=True,
    )


def test_extract_alldublin_archetype_totals(ber_with_irrelevant_col) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """dwelling_type, period_built, alldublin_archetype_total
            apartments, 1919 - 1945,  1
            terraced house, 1946 - 1960, 3"""
        ),
        skipinitialspace=True,
    )
    output = _extract_alldublin_archetype_totals.run(ber_with_irrelevant_col)
    assert_frame_equal(output, expected_output)


def test_extract_postcode_archetype_totals(ber_with_irrelevant_col) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, postcode_archetype_total
            dublin 7, apartments, 1919 - 1945,  1
            dublin 17, terraced house, 1946 - 1960, 2
            dublin 12, terraced house, 1946 - 1960, 1"""
        ),
        skipinitialspace=True,
    )
    output = _extract_postcode_archetype_totals.run(ber_with_irrelevant_col)
    assert_frame_equal(output, expected_output)


@pytest.fixture()
def ber_minimal():
    return pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built
            dublin 7, apartments, 1919 - 1945
            dublin 17, terraced house, 1946 - 1960
            dublin 17, terraced house, 1946 - 1960
            dublin 12, terraced house, 1946 - 1960"""
        ),
        skipinitialspace=True,
    )


def test_add_column_postcode_archetype_id(ber_minimal) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, postcode_archetype_id
            dublin 7, apartments, 1919 - 1945, 1
            dublin 17, terraced house, 1946 - 1960, 1
            dublin 17, terraced house, 1946 - 1960, 2
            dublin 12, terraced house, 1946 - 1960, 1"""
        ),
        skipinitialspace=True,
    )

    output = _add_column_postcode_archetype_id.run(ber_minimal)
    assert_frame_equal(output, expected_output)


def test_add_column_alldublin_archetype_id(ber_minimal) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, alldublin_archetype_id
            dublin 7, apartments, 1919 - 1945, 1
            dublin 17, terraced house, 1946 - 1960, 1
            dublin 17, terraced house, 1946 - 1960, 2
            dublin 12, terraced house, 1946 - 1960, 3"""
        ),
        skipinitialspace=True,
    )

    output = _add_column_alldublin_archetype_id.run(ber_minimal)
    assert_frame_equal(output, expected_output)


@pytest.fixture()
def census_minimal():
    return pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, postcodes
            arran quay a, terraced house, 1946 - 1960, dublin 17"""
        ),
        skipinitialspace=True,
    )


def test_link_to_ber_postcode_archetype_totals(census_minimal):

    postcode_archetype_totals = pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, postcode_archetype_total
            dublin 17, terraced house, 1946 - 1960, 2"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, postcodes, postcode_archetype_total
            arran quay a, terraced house, 1946 - 1960, dublin 17, 2"""
        ),
        skipinitialspace=True,
    )

    output = _link_to_ber_postcode_archetype_totals.run(
        census_minimal, postcode_archetype_totals,
    )
    assert_frame_equal(output, expected_output)


def test_link_to_ber_postcode_archetype_totals(census_minimal):

    alldublin_archetype_totals = pd.read_csv(
        StringIO(
            """dwelling_type, period_built, alldublin_archetype_total
            terraced house, 1946 - 1960, 3"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, postcodes, alldublin_archetype_total
            arran quay a, terraced house, 1946 - 1960, dublin 17, 3"""
        ),
        skipinitialspace=True,
    )

    output = _link_to_ber_alldublin_archetype_totals.run(
        census_minimal, alldublin_archetype_totals,
    )
    assert_frame_equal(output, expected_output)


def test_flag_small_archetype_sample_sizes() -> None:

    postcode_archetype_totals = pd.read_csv(
        StringIO(
            """postcode_archetype_total
            2
            30"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """postcode_archetype_total, postcode_archetype_sample_too_small
            2, True
            30, False"""
        ),
        skipinitialspace=True,
    )

    output = _flag_small_archetype_sample_sizes.run(postcode_archetype_totals)
    assert_frame_equal(output, expected_output)


# custom class to be the mock return value
class MockNpRandomDefaultRng:

    # Source: https://docs.pytest.org/en/latest/monkeypatch.html
    # mock integers() method always returns a specific array of integers...
    @staticmethod
    def integers(*args, **kwargs):
        return np.array([10])


def test_select_a_random_matching_postcode_archetype(monkeypatch) -> None:

    input = pd.DataFrame(
        {
            "postcode_archetype_total": [30],
            "postcode_archetype_sample_too_small": [False],
        }
    )
    expected_output = pd.DataFrame(
        {
            "postcode_archetype_total": [30],
            "postcode_archetype_sample_too_small": [False],
            "postcode_archetype_id": [10],
        }
    )

    # Any arguments may be passed and mock_np_random() will always return our
    # mocked object, which only has the .integers() method.
    def mock_numpy_random_default_rng(*args, **kwargs):
        return MockNpRandomDefaultRng()

    # apply the monkeypatch for requests.get to mock_get
    monkeypatch.setattr(np.random, "default_rng", mock_numpy_random_default_rng)

    output = _select_a_random_matching_postcode_archetype.run(input)
    assert_frame_equal(output, expected_output)


def test_link_to_matching_postcode_archetypes() -> None:

    census = pd.read_csv(
        StringIO(
            """dwelling_type, period_built, postcodes, postcode_archetype_id
            apartments, 1919 - 1945, dublin 7, 5
            terraced house, 1946 - 1960, dublin 17, 9
            terraced house, 1946 - 1960, dublin 17, 12"""
        ),
        skipinitialspace=True,
    )

    ber = pd.read_csv(
        StringIO(
            """postcodes, dwelling_type, period_built, COLX, postcode_archetype_id
            dublin 7, apartments, 1919 - 1945, X, 5
            dublin 17, terraced house, 1946 - 1960, X, 9"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """dwelling_type, period_built, postcodes, postcode_archetype_id, COLX
            apartments, 1919 - 1945, dublin 7, 5, X
            terraced house, 1946 - 1960, dublin 17, 9, X"""
        ),
        skipinitialspace=True,
    )

    output = _link_to_matching_postcode_archetypes.run(census, ber)
    assert_frame_equal(output, expected_output)
