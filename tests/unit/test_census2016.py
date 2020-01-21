import logging
import sys
from pathlib import Path

import numpy as np
import pytest
from _pytest.logging import caplog

import src
from src.helper.logging import create_logger, log_dataframe
from src.preprocess import Census2016

ROOT_DIR = Path(src.__path__[0]).parent
LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)


@pytest.fixture
def census2016_raw():

    return Census2016.load_census2016().head()


@pytest.fixture
def census2016_with_strings_replaced():

    raw_data = Census2016.load_census2016().head()

    return Census2016._replace_less_than_x_values(
        raw_data,
    )


@pytest.fixture
def census2016_balanced():

    raw_data = Census2016.load_census2016().head()
    Census2016._replace_less_than_x_values(
        raw_data,
    )
    return Census2016._replace_less_than_6_with_number(
        census2016_with_strings_replaced,
    )


def test_replace_less_than_x_values(census2016_with_strings_replaced):

    # Fails if all columns haven't been converted to np.dtype('float64')
    assert (
        np.dtype('O')
        not in census2016_with_strings_replaced.dtypes.tolist()
    )


def test_replace_less_than_6_with_number(census2016_with_strings_replaced):

    census2016_balanced = (
        Census2016._replace_less_than_6_with_number(
            census2016_with_strings_replaced,
        )
    )
    dwelling_types = [
        'All Households',
        'Detached house',
        'Semi-detached house',
        'Terraced house',
        'All Apartments and Bed-sits',
    ]

    # assert sum of each row is less than or equal to all years
    for dwelling_type in dwelling_types:
        df_view = census2016_balanced[dwelling_type]

        # for each row check if the sum of each row <= 'All Years'
        assert np.less_equal(
            df_view['All Years'].values,
            df_view.loc[:, df_view.columns != 'All Years'].sum(axis=1).values,
        ).all()


def test_distribute_not_stated_column():

    old_column = np.array([1, 10, 50])
    not_stated_column = np.array([10, 10, 10])
    row_total_column = np.array([100, 100, 100])

    expected_result = np.array([1, 11, 55])

    new_column = Census2016.distribute_not_stated_column(
        old_column, not_stated_column, row_total_column
    )

    np.testing.assert_almost_equal(new_column, expected_result, decimal=1)
