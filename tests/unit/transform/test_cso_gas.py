import pandas as pd

from pandas.testing import assert_frame_equal

from drem.transform.cso_gas import _replace_column_name_with_third_row


def test_replace_column_name_with_third_row():
    """Test replace column name with values from third row."""
    raw_data = pd.DataFrame({0: ["", "", "name", "data"]})
    expected_output = pd.DataFrame({"name": ["data"]})

    output = _replace_column_name_with_third_row.run(raw_data)

    assert_frame_equal(output, expected_output)
