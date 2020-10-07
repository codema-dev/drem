import pandas as pd
import pytest

from icontract import ViolationError
from pandas.testing import assert_frame_equal

from drem.transform.dublin_postcodes import _rename_postcodes


def test_rename_postcodes() -> None:
    """Rename postcodes replaces only Dublin <number> postcodes."""
    postcodes = pd.DataFrame(
        {
            "postcodes": [
                "Dublin 1",
                "North County Dublin",
                "South County Dublin",
                "Phoenix Park",
            ],
        },
    )
    expected_output = pd.DataFrame(
        {"postcodes": ["Dublin 1", "Co. Dublin", "Co. Dublin", "Co. Dublin"]},
    )

    output = _rename_postcodes(postcodes)
    assert_frame_equal(output, expected_output)


def test_rename_postcodes_raises_error() -> None:
    """Call _rename_postcodes raises error."""
    postcodes = pd.DataFrame({"pcodes": ["Dublin 1"]})
    with pytest.raises(ViolationError):
        _rename_postcodes(postcodes)
