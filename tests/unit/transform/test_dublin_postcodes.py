from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from geopandas.testing import assert_geodataframe_equal
from icontract import ViolationError
from pandas.testing import assert_frame_equal

import drem

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.dublin_postcodes import _rename_postcodes


POSTCODES_IN: Path = UTEST_DATA_TRANSFORM / "dublin_postcodes_raw.parquet"
POSTCODES_EOUT: Path = UTEST_DATA_TRANSFORM / "dublin_postcodes_clean.parquet"


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


def test_transform_dublin_postcodes() -> None:
    """Transformed Dublin postcodes match reference file."""
    postcodes_raw = gpd.read_parquet(POSTCODES_IN)

    output = drem.transform_dublin_postcodes.run(postcodes_raw)

    expected_output = gpd.read_parquet(POSTCODES_EOUT)
    assert_geodataframe_equal(output, expected_output)
