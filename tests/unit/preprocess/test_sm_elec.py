from io import StringIO
from pathlib import Path

import dask.dataframe as dpd
import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import codema_drem
from codema_drem.preprocess.SM_elec import (
    _convert_to_datetime,
    _load_sm_elec_raw_txt_into_dask,
    _parse_sm_elec_data,
)


def test_parse_sm_elec_data():

    input = pd.read_table(
        StringIO(
            """1392 19503 0.14
            1392 19503 0.14"""
        ),
        skipinitialspace=True,
        names=["id", "timeid", "demand"],
        delim_whitespace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """id, demand, day, halfhourly_id
            1392, 0.14, 195, 03
            1392, 0.14, 195, 03"""
        ),
        skipinitialspace=True,
    )

    output = _parse_sm_elec_data.run(input)
    assert_frame_equal(output, expected_output)


def test_convert_to_datetime() -> None:

    input = pd.read_csv(
        StringIO(
            """id, demand, day, halfhourly_id,
            1392,  0.14, 195, 03
            1392,  0.14, 25, 03"""
        ),
        skipinitialspace=True,
    )

    expected_output = pd.read_csv(
        StringIO(
            """id, demand, datetime
            1392, 0.14, 15/07/2009 01:30:00 
            1392, 0.14, 26/01/2009 01:30:00"""
        ),
        skipinitialspace=True,
        parse_dates=["datetime"],
    )

    output = _convert_to_datetime.run(input)
    assert_frame_equal(output, expected_output)
