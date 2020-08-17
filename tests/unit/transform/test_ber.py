import pandas as pd

from pandas.testing import assert_frame_equal

from drem.transform.ber import _bin_year_of_construction_as_in_census
from drem.transform.ber import _extract_dublin_rows


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
