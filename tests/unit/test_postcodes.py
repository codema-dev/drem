import pytest
import numpy as np
import pandas as pd
from typing import List

import src.join.BER_and_Postcodes as test


@pytest.fixture
def _dublin_postcodes() -> List[np.array]:

    # ensure all postcodes (except 19, 21 & 23) are in data
    return np.array([
        'co. dublin',
        'dublin 1',
        'dublin 2',
        'dublin 3',
        'dublin 4',
        'dublin 5',
        'dublin 6',
        'dublin 6w',
        'dublin 7',
        'dublin 8',
        'dublin 9',
        'dublin 11',
        'dublin 12',
        'dublin 13',
        'dublin 14',
        'dublin 15',
        'dublin 16',
        'dublin 17',
        'dublin 18',
        'dublin 19',
        'dublin 21',
        'dublin 23',
        'dublin 24',
    ])


def test_
