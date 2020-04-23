from pathlib import Path

import dask.dataframe as dpd
import numpy as np
import pandas as pd
from pipeop import pipes
from prefect import Flow, task

from codema_drem._filepaths import (
    BER_CLEAN,
    SM_GAS_RES_DEMANDS_CLEAN,
    SM_GAS_RES_PROFILES_CLEAN,
)
from codema_drem.utilities.flow import run_flow


# Flows
# *****


@pipes
def compare_sm_gas_to_ber_demands() -> Flow:

    with Flow("Compare Smart Meter Gas demands to BER") as flow:

        ber = _load_parquet(BER_CLEAN)
        sm_gas_demands = _load_parquet(SM_GAS_RES_DEMANDS_CLEAN)
        sm_gas_survey = _load_parquet(SM_GAS_RES_PROFILES_CLEAN)

        sm_gas_profiles = (
            _set_datetime_as_index(sm_gas_demands)
            >> _sort_datetime_index
            >> _extract_one_year_of_demands
            >> _calculate_annual_demands_by_id
            >> _link_profiles_to_annual_demands(sm_gas_survey)
        )

    return flow


# Tasks
# *****


@task
def _load_parquet(filepath: Path) -> pd.DataFrame:

    return pd.read_parquet(filepath)


@task
def _set_datetime_as_index(gas_demands: pd.DataFrame) -> pd.DataFrame:

    return gas_demands.set_index("datetime").sort_index()


@task
def _sort_datetime_index(gas_demands: pd.DataFrame) -> pd.DataFrame:

    return gas_demands.sort_index()


@task
def _extract_one_year_of_demands(gas_demands: pd.DataFrame) -> pd.DataFrame:

    return gas_demands.loc["2010-01-01":"2010-12-31"]


@task
def _calculate_annual_demands_by_id(gas_demands: pd.DataFrame) -> pd.DataFrame:

    return gas_demands.groupby("id").sum()


@task
def _link_profiles_to_annual_demands(
    gas_demands: pd.DataFrame, gas_profiles: pd.DataFrame,
) -> pd.DataFrame:

    return pd.merge(gas_demands, gas_profiles, left_index=True, right_on="id")


@task
def _bin_gas_demands_into_regulation_periods(
    gas_demands: pd.DataFrame,
) -> pd.DataFrame:

    return gas_demands.assign(
        regulatory_period=pd.cut(
            gas_demands["year_of_construction"], [-np.inf, 1978, 1991, 2006, np.inf]
        )
    )


@task
def _bin_bers_into_regulation_periods(ber: pd.DataFrame,) -> pd.DataFrame:

    return ber.assign(
        regulatory_period=pd.cut(
            ber["Year of Construction"], [-np.inf, 1978, 1991, 2006, np.inf]
        )
    )
