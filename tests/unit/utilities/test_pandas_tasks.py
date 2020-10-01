import pandas as pd
import pytest

from icontract import ViolationError
from pandas.testing import assert_frame_equal

import drem.utilities.pandas_tasks as pdt


def test_get_columns_raises_error_if_passed_nonexistent_column_name() -> None:
    """Raise error if passed non-existent column name."""
    i_am_data = pd.DataFrame({"my_name_is": ["what"]})

    with pytest.raises(ViolationError):
        pdt.get_columns.run(i_am_data, ["my_name_is", "i_dont_exist"])


def test_get_columns_raises_error_if_passed_non_dataframe() -> None:
    """Raise error if passed non-existent column name."""
    i_am_not_a_dataframe = 12

    with pytest.raises(ViolationError):
        pdt.get_columns.run(i_am_not_a_dataframe, ["my_name_is", "i_dont_exist"])


def test_sum_columns_raises_error_if_passed_nonexistent_column_name() -> None:
    """Raise error if passed non-existent column name."""
    i_am_data = pd.DataFrame({"my_name_is": ["what"]})

    with pytest.raises(ViolationError):
        pdt.get_sum_of_columns.run(i_am_data, ["my_name_is", "i_dont_exist"])


def test_get_rows_where_column_contains_substring_matches_expected() -> None:
    """Get rows where column row matches substring."""
    before_filtering = pd.DataFrame(
        {"postcodes": ["Co. Dublin", "Co. Wicklow", "Dublin 1"]},
    )
    expected_output = pd.DataFrame({"postcodes": ["Co. Dublin", "Dublin 1"]})

    output = pdt.get_rows_where_column_contains_substring(
        before_filtering, target="postcodes", substring="Dublin",
    )

    assert_frame_equal(output, expected_output)
