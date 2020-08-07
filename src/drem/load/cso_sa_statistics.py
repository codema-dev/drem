#!/usr/bin/env python

import pandas as pd

from prefect import task

from drem.filepaths import PROCESSED_DIR


@task(name="Load Small Area Statistics to file")
def load_cso_sa_statistics(statistics: pd.DataFrame) -> None:
    """Load transformed Dublin Small Area Statistics data to local file.

    Args:
        statistics (pd.DataFrame): Transformed Dublin Small Area Statistics data
    """
    savepath = PROCESSED_DIR / "small_area_statistics.csv"
    statistics.to_csv(savepath)
