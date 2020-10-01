import pandas as pd
import pytest

from icontract import ViolationError

import drem.utilities.pandas_tasks as pdt


def test_get_columns_raises_error_if_passed_nonexistent_column_name() -> None:
    """Raise error if passed non-existent column name."""
    i_am_data = pd.DataFrame({"my_name_is": ["what"]})

    with pytest.raises(ViolationError):
        pdt.get_columns.run(i_am_data, ["my_name_is", "i_dont_exist"])


def test_get_columns_raises_error_for_multiindex_df_index() -> None:
    """Raise error if index is MultiIndex."""
    i_am_data = pd.DataFrame({"my_name_is": ["what"]})

    with pytest.raises(ViolationError):
        pdt.get_columns.run(i_am_data, ["my_name_is", "i_dont_exist"])
