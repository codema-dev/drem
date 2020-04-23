from io import StringIO
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from codema_drem.preprocess.Census2016 import (
    _remove_numbers_from_eds,
    _replace_all_less_than_six_values_with_zero,
    _replace_all_other_less_than_values_with_themselves,
    _infer_values_so_sum_of_dwelling_types_is_approx_equal_to_total,
    _infer_values_so_sum_of_periods_built_is_approx_equal_to_total,
)


# TODO: rewrite below functions for a prefect flow...


def test_remove_numbers_from_eds() -> None:

    eds = pd.read_csv(
        StringIO(
            """eds
            02001Arran Quay A
            05069Tibradden"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds
            Arran Quay A
            Tibradden"""
        ),
        skipinitialspace=True,
    )

    output = _remove_numbers_from_eds.run(eds)
    assert_frame_equal(output, expected_output)


def test_initialise_less_than_6_values_with_zero() -> None:

    census = pd.read_csv(
        StringIO(
            """period_built, dwelling_type, total_hh
            all years, apartments, <6"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """period_built, dwelling_type, total_hh
            all years, apartments, 0"""
        ),
        skipinitialspace=True,
    )

    output = _replace_all_less_than_six_values_with_zero.run(census)
    assert_frame_equal(output, expected_output)


def test_replace_all_other_less_than_values_with_themselves() -> None:

    census = pd.read_csv(
        StringIO(
            """period_built, dwelling_type, total_hh
            all years, apartments, <300"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """period_built, dwelling_type, total_hh
            all years, apartments, 300"""
        ),
        dtype={"total_hh": np.uint16},
        skipinitialspace=True,
    )

    output = _replace_all_other_less_than_values_with_themselves.run(census)
    assert_frame_equal(output, expected_output)


def test_infer_values_so_sum_of_dwelling_types_is_approx_equal_to_total() -> None:

    census = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            airport, apartments, before 1919, 0
            airport, detached house, before 1919, 7
            airport, all households, before 1919, 10"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            airport, apartments, before 1919, 3
            airport, detached house, before 1919, 7
            airport, all households, before 1919, 10"""
        ),
        skipinitialspace=True,
    )

    output = _infer_values_so_sum_of_dwelling_types_is_approx_equal_to_total.run(census)
    assert_frame_equal(output, expected_output)


def test_infer_values_so_sum_of_periods_built_is_approx_equal_to_total() -> None:

    census = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            airport, apartments, 1981 - 1990, 0
            airport, apartments, before 1919, 7
            airport, apartments, all years, 10"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """eds, dwelling_type, period_built, total_hh
            airport, apartments, 1981 - 1990, 0
            airport, apartments, before 1919, 7
            airport, apartments, all years, 10"""
        ),
        skipinitialspace=True,
    )

    output = _infer_values_so_sum_of_periods_built_is_approx_equal_to_total.run(census)
    assert_frame_equal(output, expected_output)


# def test_replace_zeros_so_sum_of_dwelling_types_is_approx_equal_total() -> None:

#     census = pd.read_csv(
#         StringIO(
#             """eds, period_built, apartments, detached house, all households
#             airport, 1981 - 1990, 0, 7, 10"""
#         ),
#         skipinitialspace=True,
#     )

#     expected_output = pd.read_csv(
#         StringIO(
#             """eds, period_built, apartments, detached house, all households
#             airport, 1981 - 1990, 3, 7, 10"""
#         ),
#         skipinitialspace=True,
#     )

#     output = _replace_zeros_so_sum_of_dwelling_types_is_approx_equal_total.run(census)
#     assert_frame_equal(output, expected_output)


# def test_replace_zeros_so_sum_of_periods_built_is_approx_equal_total() -> None:

#     census = pd.read_csv(
#         StringIO(
#             """eds, period_built, apartments
#             airport, before 1919, 0
#             airport, 1981 - 1990, 7
#             airport, all years, 10"""
#         ),
#         skipinitialspace=True,
#     )

#     expected_output = pd.read_csv(
#         StringIO(
#             """eds, period_built, apartments
#             airport, before 1919, 3
#             airport, 1981 - 1990, 7
#             airport, all years, 10"""
#         ),
#         skipinitialspace=True,
#     )

#     output = _replace_zeros_so_sum_of_periods_built_is_approx_equal_total.run(census)
#     assert_frame_equal(output, expected_output)


# @pipes
# def test_infer_values_where_less_than_six() -> None:

#     census = pd.read_csv(
#         StringIO(
#             """eds, period_built, apartments, detached house, all households
#             airport, 1981 - 1990, 0, 7, 10
#             airport, before 1919, 0, 7, 10
#             airport, all years, 10,
#             """
#         ),
#         skipinitialspace=True,
#     )

#     expected_output = pd.read_csv(
#         StringIO(
#             """"eds, period_built, apartments, detached house, all households
#             airport, 1981 - 1990, 4, 7, 10
#             airport, before 1919, 4, 7, 10
#             airport, all years, 10, 14, 20"""
#         ),
#         skipinitialspace=True,
#     )

#     output = (
#         _replace_zeros_so_sum_of_dwelling_types_is_approx_equal_total.run(census)
#         >> _replace_zeros_so_sum_of_periods_built_is_approx_equal_total
#     )


# @pytest.fixture
# def census2016_raw():

#     return Census2016.load_census2016().head()


# @pytest.fixture
# def census2016_with_strings_replaced():

#     raw_data = Census2016.load_census2016().head()

#     return Census2016._replace_less_than_x_values(
#         raw_data,
#     )


# @pytest.fixture
# def census2016_balanced():

#     raw_data = Census2016.load_census2016().head()
#     Census2016._replace_less_than_x_values(
#         raw_data,
#     )
#     return Census2016._replace_less_than_6_with_number(
#         census2016_with_strings_replaced,
#     )


# def test_replace_less_than_x_values(census2016_with_strings_replaced):

#     # Fails if all columns haven't been converted to np.dtype('float64')
#     assert (
#         np.dtype('O')
#         not in census2016_with_strings_replaced.dtypes.tolist()
#     )


# def test_replace_less_than_6_with_number(census2016_with_strings_replaced):

#     census2016_balanced = (
#         Census2016._replace_less_than_6_with_number(
#             census2016_with_strings_replaced,
#         )
#     )
#     dwelling_types = [
#         'All Households',
#         'Detached house',
#         'Semi-detached house',
#         'Terraced house',
#         'All Apartments and Bed-sits',
#     ]

#     # assert sum of each row is less than or equal to all years
#     for dwelling_type in dwelling_types:
#         df_view = census2016_balanced[dwelling_type]

#         # for each row check if the sum of each row <= 'All Years'
#         assert np.less_equal(
#             df_view['All Years'].values,
#             df_view.loc[:, df_view.columns != 'All Years'].sum(axis=1).values,
#         ).all()


# def test_distribute_not_stated_column():

#     old_column = np.array([1, 10, 50])
#     not_stated_column = np.array([10, 10, 10])
#     row_total_column = np.array([100, 100, 100])

#     expected_result = np.array([1, 11, 55])

#     new_column = Census2016.distribute_not_stated_column(
#         old_column, not_stated_column, row_total_column
#     )

#     np.testing.assert_almost_equal(new_column, expected_result, decimal=1)
