import pandas as pd

from pandas.testing import assert_frame_equal
from tdda.referencetest.referencetest import ReferenceTest

import drem

from drem.filepaths import UTEST_DATA_TRANSFORM
from drem.transform.ber import _bin_year_of_construction_as_in_census
from drem.transform.ber import _extract_dublin_rows


BER_RAW = UTEST_DATA_TRANSFORM / "ber_raw.parquet"
BER_CLEAN = UTEST_DATA_TRANSFORM / "ber_clean.csv"


def test_extract_dublin_rows() -> None:
    """Extract Dublin rows from Ireland dataframe."""
    ber_ireland: pd.DataFrame = pd.DataFrame(
        {"CountyName": ["Dublin 11", "Co. Dublin", "Co. Cork"]},
    )

    expected_output: pd.DataFrame = pd.DataFrame(
        {"CountyName": ["Dublin 11", "Co. Dublin"]},
    )

    output: pd.DataFrame = _extract_dublin_rows(ber_ireland)
    assert_frame_equal(output, expected_output)


def test_bin_year_of_construction_as_in_census() -> None:
    """Bin construction year into Census 2016 compatible bins."""
    ber: pd.DataFrame = pd.DataFrame({"Year_of_Construction": ["1891", "2005", "2015"]})

    expected_output: pd.DataFrame = pd.DataFrame(
        {
            "Year_of_Construction": ["1891", "2005", "2015"],
            "period_built": ["before 1919", "2001 - 2010", "2011 or later"],
        },
    )

    output: pd.DataFrame = _bin_year_of_construction_as_in_census(ber)

    assert_frame_equal(output, expected_output)


def test_transform_ber(ref: ReferenceTest) -> None:
    """Transformed ber matches reference data.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    ber_raw = pd.read_parquet(BER_RAW)

    """ DOES:
        - Extract columns: Year of Construction, Dwelling Type, Energy ...
        - Extract Dublin rows
        - Map Period Built data to National Regulatory Periods
        - Calculate Total Heat Demand
        - Extract columns: Period Built, Dwelling Type, Total Heat Demand
    """
    ber_clean = drem.transform_ber.run(ber_raw)

    ref.assertDataFrameCorrect(ber_clean, BER_CLEAN)
