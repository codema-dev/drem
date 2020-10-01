import pandas as pd

from pandas.testing import assert_frame_equal

from drem.transform.ber import _bin_year_of_construction_as_in_census


def test_bin_year_of_construction_as_in_census() -> None:
    """Bin construction year into Census 2016 compatible bins."""
    ber: pd.DataFrame = pd.DataFrame({"Year_of_Construction": ["1891", "2005", "2015"]})

    expected_output: pd.DataFrame = pd.DataFrame(
        {
            "Year_of_Construction": ["1891", "2005", "2015"],
            "cso_period_built": ["before 1919", "2001 - 2010", "2011 or later"],
        },
    )

    output: pd.DataFrame = _bin_year_of_construction_as_in_census.run(
        ber, target="Year_of_Construction", result="cso_period_built",
    )

    assert_frame_equal(output, expected_output)
