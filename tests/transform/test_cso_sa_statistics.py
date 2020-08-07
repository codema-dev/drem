from pathlib import Path

import pandas as pd

from pandas.testing import assert_frame_equal

from drem.transform.cso_sa_statistics import _clean_year_built_columns
from drem.transform.cso_sa_statistics import _extract_year_built
from drem.transform.cso_sa_statistics import _melt_year_built_columns


CWD = Path.cwd()

STATISTICS_IN = CWD / "test_data" / "cso_sa_statistics_in.csv"
GLOSSARY = CWD / "test_data" / "glossary.xlsx"

EXTRACT_EOUT = CWD / "test_data" / "extract_year_built_eout.csv"
MELT_EOUT = CWD / "test_data" / "melt_year_built_eout.csv"
CLEANED_EOUT = CWD / "test_data" / "clean_year_built.csv"
STATISTICS_EOUT = CWD / "test_data" / "cso_sa_statistics_eout"


def test_extract_year_built() -> None:
    """Extracted columns match reference file."""
    statistics = pd.read_csv(STATISTICS_IN)
    glossary = pd.read_excel(GLOSSARY, engine="openpyxl")

    output = _extract_year_built(statistics, glossary)

    expected_output = pd.read_csv(EXTRACT_EOUT)
    assert_frame_equal(output, expected_output, check_like=True)


def test_melt_year_built_columns() -> None:
    """Melted columns match reference file."""
    statistics = pd.read_csv(EXTRACT_EOUT)

    output = _melt_year_built_columns(statistics)

    expected_output = pd.read_csv(MELT_EOUT)
    assert_frame_equal(output, expected_output, check_like=True)


def test_clean_year_built_columns() -> None:
    """Cleaned columns match reference file."""
    statistics = pd.read_csv(MELT_EOUT)

    output = _clean_year_built_columns(statistics)

    expected_output = pd.read_csv(CLEANED_EOUT)
    assert_frame_equal(output, expected_output, check_like=True)
