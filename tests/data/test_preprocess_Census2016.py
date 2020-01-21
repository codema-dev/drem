import pytest
import numpy as np

from src.preprocess import Census2016

# Properties to test:
# - Total of all HH row < Total HH (i.e. can't assume <6==6 ...)

# @pytest.fixture
# def Census2016_data():
#
#     df_raw, _, __ = preprocess_Census2016.load_Census2016_data()
#     return df_raw


def test_distribute_not_stated_column():

    old_column = np.array([1, 10, 50])
    not_stated_column = np.array([10, 10, 10])
    row_total_column = np.array([100, 100, 100])

    expected_result = np.array([1, 11, 55])

    new_column = Census2016.distribute_not_stated_column(
        old_column, not_stated_column, row_total_column
    )

    np.testing.assert_almost_equal(new_column, expected_result, decimal=1)


# def test_redistribute_not_stated_Dwelling_Type():
