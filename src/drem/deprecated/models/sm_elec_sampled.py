from pathlib import Path
from typing import List

import dask.dataframe as dpd
from faker import Faker
import numpy as np
import pandas as pd
from pipeop import pipes
from prefect import Flow, task

from codema_drem._filepaths import SM_ELEC_DEMANDS_CLEAN_PARQUET_DIR
from codema_drem.utilities.flow import run_flow


SAMPLE_SIZES = [10, 50, 100, 500, 1000]

# Flows
# *****
@pipes
def compare_sm_elec_peak_for_different_sample_sizes() -> Flow:

    with Flow("Compare Peak Electricity demands across samples") as flow:

        sm_elec = _load_parquet(SM_ELEC_DEMANDS_CLEAN_PARQUET_DIR)

    return Flow


# Tasks
# *****
@task
def _load_dask_parquet(filepath: Path) -> dpd.DataFrame:

    return dpd.read_parquet(filepath)


@task
def _get_unique_building_ids(sm_elec: dpd.DataFrame,) -> pd.Series:

    return sm["id"].unique().compute()


@task
def _sample_n_buildings(
    sm_elec: dpd.DataFrame, building_ids: pd.Series, sample_size: List[int]
) -> dpd.DataFrame:

    sample_ids = building_ids.sample(sample_size).values
    pulling_matching_ids = np.isin(sm_elec["id"].values, sample_ids).compute()
    return sm_elec[pulling_matching_ids]


@task
def _pick_a_random_day(sm_elec: dpd.DataFrame) -> dpd.DataFrame:

    fake = Faker()

    # Generate a fake day between the first and last dates
    random_day = fake.date_between(
        sm_elec.head(1).index.date[0], sm_elec.tail(1).index.date[0]
    ).isoformat()

    return sm_elec.loc[random_day]


@task
def _calculate_relative_peak_demand(sm_elec: dpd.DataFrame) -> dpd.DataFrame:

    return sm_elec["demand"].sum() / sm_elec["id"].nunique()

