import pytest
import numpy as np
import pandas as pd
import geopandas as gpd
from typing import List

from pandas.testing import assert_frame_equal

from codema_drem.preprocess.Dublin_Postcodes import (
    _set_non_numeric_postcodes_to_dublin_county,
    run_postcode_etl_flow,
    POSTCODES_IN,
    POSTCODES_OUT,
)


def test_set_non_numeric_postcodes_to_dublin_county():

    postcodes = pd.DataFrame([
        'phoenix park',
        'north county dublin',
        'dublin 3',
    ], columns=['postcodes'])

    expected_result = pd.DataFrame([
        'co. dublin',
        'co. dublin',
        'dublin 3',
    ], columns=['postcodes'])

    result = _set_non_numeric_postcodes_to_dublin_county.run(postcodes)

    assert_frame_equal(result, expected_result)


def test_output_contains_all_dublin_postcodes() -> List[np.array]:

    # ensure all postcodes are in raw data
    # NOTE ber mistakenly contains except 19, 21 & 23 which don't exist...
    expected_postcodes = np.array([
        'co. dublin',
        'dublin 1',
        'dublin 2',
        'dublin 3',
        'dublin 4',
        'dublin 5',
        'dublin 6',
        'dublin 6w',
        'dublin 7',
        'dublin 8',
        'dublin 9',
        'dublin 10',
        'dublin 11',
        'dublin 12',
        'dublin 13',
        'dublin 14',
        'dublin 15',
        'dublin 16',
        'dublin 17',
        'dublin 18',
        'dublin 19',
        'dublin 20',
        'dublin 21',
        'dublin 22',
        'dublin 23',
        'dublin 24',
    ])

    postcodes = gpd.read_file(POSTCODES_OUT)

    difference = np.setdiff1d(
        postcodes['postcodes'].array,
        expected_postcodes,
    )
    assert difference.size == 0


def test_flow_run_is_successful():

    state = run_postcode_etl_flow()
    assert state.is_failed() is False


@pytest.mark.slow
def test_flow_run_is_successful():

    state, _ = run_postcode_etl_flow()
    assert state.is_successful()
