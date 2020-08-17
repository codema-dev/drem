import pandas as pd

from tdda.referencetest.referencetest import ReferenceTest

import drem

from drem.filepaths import FTEST_DATA_TRANSFORM


BER_RAW = FTEST_DATA_TRANSFORM / "ber_raw.parquet"
BER_CLEAN = FTEST_DATA_TRANSFORM / "ber_clean.csv"


def test_transform_ber(ref: ReferenceTest) -> None:
    """Transformed ber matches reference data.

    Args:
        ref (ReferenceTest): a tdda plugin used to verify a DataFrame against a file.
    """
    ber_raw = pd.read_parquet(BER_RAW)

    """ DOES:
        - Extract columns: Year of Construction, Dwelling Type, Energy ...
        - Extract Dublin rows
        - Map Period Built data to Census 2016
        - Calculate Total Heat Demand
        - Extract columns: Period Built, Dwelling Type, Total Heat Demand
    """
    ber_clean = drem.transform_ber.run(ber_raw)

    ref.assertDataFrameCorrect(ber_clean, BER_CLEAN)
