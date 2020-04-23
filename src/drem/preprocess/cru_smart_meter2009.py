from shutil import rmtree
from pathlib import Path
from typing import Union

import dask.dataframe as dpd
import numpy as np
import pandas as pd
from pipeop import pipes
from prefect import Flow, task
from prefect import context
from sqlalchemy import create_engine
from tqdm import tqdm

from codema_drem.utilities.flow import run_flow
from codema_drem._filepaths import (
    DATA_DIR,
    SM_ELEC_DEMANDS_RAW_DIR,
    SM_ELEC_DEMANDS_CLEAN_PARQUET_DIR,
    SM_ELEC_DEMANDS_CLEAN_CSV_DIR,
    SM_ELEC_RES_PROFILES_CLEAN,
    SM_ELEC_RES_PROFILES_RAW,
    SM_ELEC_SME_PROFILES_RAW,
    SM_ELEC_SME_PROFILES_CLEAN,
    SM_GAS_DEMANDS_RAW_DIR,
    SM_GAS_DEMANDS_CLEAN_PARQUET_DIR,
    SM_GAS_DEMANDS_CLEAN_CSV_DIR,
    SM_GAS_RES_PROFILES_RAW,
    SM_GAS_RES_PROFILES_CLEAN,
    SM_GAS_RES_DEMANDS_CLEAN,
)

SM_ELEC_DEMANDS_SQL = DATA_DIR / "interim" / "sm_elec.db"


@pipes
def smart_meter_demands_etl_flow():

    with Flow("Preprocess Smart Meter demands") as flow:

        sm_elec_demands_clean = (
            _load_raw_sm_elec_demands(SM_ELEC_DEMANDS_RAW_DIR)
            >> _parse_sm_demands
            >> _convert_to_datetime
            >> _reorder_columns
        )
        _set_datetime_index_and_save_to_dask_parquet(
            sm_elec_demands_clean, SM_ELEC_DEMANDS_CLEAN_PARQUET_DIR
        )
        # _save_each_id_to_csv(sm_elec_demands_clean, SM_ELEC_DEMANDS_CLEAN_CSV_DIR)

        # sm_gas_demands_clean = (
        #     _load_raw_sm_gas_demands(SM_GAS_DEMANDS_RAW_DIR)
        #     >> _parse_sm_demands
        #     >> _convert_to_datetime
        #     >> _reorder_columns
        #     >> _convert_to_pandas
        #     >> _set_datetime_as_index
        # )
        # _save_to_parquet(sm_gas_demands_clean, SM_GAS_RES_DEMANDS_CLEAN)
        # # _save_each_id_to_csv(sm_gas_demands_clean, SM_GAS_DEMANDS_CLEAN_CSV_DIR)

    return flow


@pipes
def smart_meter_profiles_etl_flow():

    with Flow("Preprocess Smart Meter profiles") as flow:

        gas_profiles_raw = _load_raw_sm_profiles(SM_GAS_RES_PROFILES_RAW)
        gas_profiles_clean = (
            _initialise_empty_dataframe()
            >> _extract_dwelling_id(gas_profiles_raw)
            >> _extract_number_of_occupants_in_res(gas_profiles_raw)
            >> _extract_dwelling_type(gas_profiles_raw)
            >> _extract_year_of_construction(gas_profiles_raw)
            >> _extract_floor_area(gas_profiles_raw)
            >> _extract_typical_hh_temperature(gas_profiles_raw)
            >> _extract_ownership(gas_profiles_raw)
        )
        _save_to_parquet(gas_profiles_clean, SM_GAS_RES_PROFILES_CLEAN)

    return flow


# *******
# Demands
# *******


@task
def _load_raw_sm_elec_demands(dirpath: Path) -> dpd.DataFrame:

    filepaths = list(dirpath.glob("*.txt"))
    sm_ddfs = [
        dpd.read_table(
            filepath,
            names=["id", "timeid", "demand"],
            dtype={"id": np.uint16, "timeid": np.object, "demand": np.float32},
            delim_whitespace=True,
        )
        for filepath in filepaths
    ]
    return dpd.concat(sm_ddfs)


@task
def _load_raw_sm_gas_demands(dirpath: Path) -> dpd.DataFrame:

    filepaths = list(dirpath.glob("*.txt"))
    sm_ddfs = [
        dpd.read_csv(
            filepath,
            skiprows=1,
            names=["id", "timeid", "demand"],
            dtype={"id": np.uint16, "timeid": np.object, "demand": np.float32},
        )
        for filepath in filepaths
    ]
    return dpd.concat(sm_ddfs)


@task
def _parse_sm_demands(smart_meter_data: dpd.DataFrame,) -> dpd.DataFrame:

    return smart_meter_data.assign(
        day=smart_meter_data["timeid"].astype(str).str[:3].astype(np.uint16),
        halfhourly_id=smart_meter_data["timeid"].astype(str).str[3:].astype(np.uint8),
    ).drop(columns="timeid")


@task
def _convert_to_datetime(smart_meter_data: dpd.DataFrame,) -> dpd.DataFrame:

    # NOTE: divide halfhourly_id by 2 to get an hourly id...
    return smart_meter_data.assign(
        datetime=dpd.to_datetime(smart_meter_data["day"], origin="01/01/2009", unit="D")
        + dpd.to_timedelta(smart_meter_data["halfhourly_id"] / 2, unit="h")
    ).drop(columns=["day", "halfhourly_id"])


@task
def _reorder_columns(
    smart_meter_data: dpd.DataFrame,
) -> Union[dpd.DataFrame, pd.DataFrame]:

    return smart_meter_data[["id", "datetime", "demand"]]


@task
def _set_datetime_as_index(
    smart_meter_data: Union[pd.DataFrame, dpd.DataFrame],
) -> Union[dpd.DataFrame, pd.DataFrame]:

    return smart_meter_data.set_index("datetime")


@task
def _set_index_and_save_to_dask_parquet(
    smart_meter_data: dpd.DataFrame, dirpath: Path
) -> None:

    if dirpath.exists():
        rmtree(dirpath)

    dirpath.mkdir()

    smart_meter_data.set_index("datetime").to_parquet(dirpath)


@task
def _convert_to_pandas(smart_meter_data: pd.DataFrame) -> pd.DataFrame:

    return smart_meter_data.compute()


@task
def _save_to_parquet(smart_meter_data: pd.DataFrame, dirpath: Path) -> None:

    smart_meter_data.to_parquet(dirpath)


# NOTE: Dask doesn't support multiindex operations...
# so must use Pandas to sort by datetime...


@task
def _save_each_id_to_csv(
    smart_meter_data: Union[dpd.DataFrame, pd.DataFrame], dirpath: Path
) -> None:

    for id in smart_meter_data["id"].unique():
        smart_meter_data[smart_meter_data["id"] == id].to_csv(
            str(dirpath / f"{id}.csv"), single_file=True
        )


# *******
# Profiles
# *******


@task
def _load_raw_sm_profiles(filepath: Path) -> pd.DataFrame:

    return pd.read_csv(filepath, encoding="latin1", low_memory=False)


@task
def _initialise_empty_dataframe() -> pd.DataFrame:

    return pd.DataFrame()


@task
def _extract_dwelling_id(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    return clean_profiles.assign(id=raw_profiles["ID"])


@task
def _extract_number_of_occupants_in_res(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "Question 420: How many people over 15 years of age live in your home?",
        "Question 43111: How many people under 15 years of age live in your home?",
    ]

    return clean_profiles.assign(
        number_of_occupants=(
            raw_profiles[columns].replace({" ": 0}).astype(np.int8).sum(axis=1)
        ),
    )


@task
def _extract_number_of_occupants_in_sme_elec(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 612: How many employees are there in your business at <address>?"

    return clean_profiles.assign(
        number_of_occupants=(raw_profiles[column].astype(np.int8)),
    )


@task
def _extract_dwelling_type(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 450: I would now like to ask some questions about your home.  Which best describes your home?"

    decodings = {
        1: "Apartment",
        2: "Semi-detached house",
        3: "Detached house",
        4: "Terraced house",
        5: "Bungalow",
        6: "Refused",
    }

    return clean_profiles.assign(dwelling_type=raw_profiles[column].map(decodings))


@task
def _extract_year_of_construction(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "Question 453: What year was your house or apartment built INT ENTER FOR EXAMPLE: 1981- CAPTURE THE FOUR DIGITS",
        "Question 4531: Approximately how old is your home?",
    ]

    raw_profiles_edited = pd.DataFrame().assign(
        year_built=raw_profiles[columns[0]].replace({9999: np.nan}).astype(np.float16),
        age_of_house=raw_profiles[columns[1]].replace({" ": np.nan}).astype(np.float16),
    )

    year_of_guess = 2009

    return clean_profiles.assign(
        year_of_construction=np.where(
            raw_profiles_edited["year_built"].notna(),
            raw_profiles_edited["year_built"],
            year_of_guess - raw_profiles_edited["age_of_house"],
        )
    )


@task
def _extract_floor_area(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "Question 6103: What is the approximate floor area of your home?",
        "Question 61031: Is that",
    ]

    decoding = {"1": "m**2", "2": "ft**2"}

    raw_profiles_edited = pd.DataFrame().assign(
        floor_area=raw_profiles[columns[0]]
        .replace({999999999: np.nan})
        .astype(np.float32),
        unit=raw_profiles[columns[1]].replace({" ": np.nan}).map(decoding),
    )

    ft_to_m_conv_fac = 10.764

    return clean_profiles.assign(
        floor_area=np.where(
            raw_profiles_edited["unit"] == "m**2",
            raw_profiles_edited["floor_area"],
            raw_profiles_edited["floor_area"] / ft_to_m_conv_fac,
        )
    )


@task
def _extract_typical_hh_temperature(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 2150: What temperature is your home mostly set at?"

    decodings = {
        "1": "<18 degrees",
        "2": "18 degrees-20 degrees",
        "3": "21 degrees",
        "4": "22-24 degrees",
        "5": ">24 degrees",
        "6": "Don’t know",
    }

    return clean_profiles.assign(
        typical_temp=raw_profiles[column].replace({" ": np.nan}).map(decodings)
    )


@task
def _extract_ownership(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 452: Do you own or rent your home?"

    decodings = {
        1: "Rent (from a private landlord)",
        2: "Rent (from a local authority)",
        3: "Own Outright (not mortgaged)",
        4: "Own with mortgage etc",
        5: "Other",
    }

    return clean_profiles.assign(ownership=raw_profiles[column].map(decodings))


@task
def _extract_other_non_gas_heating(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    columns = {
        "electricity": "Question 470: Which of the following do you also use?: Electricity (electric central heating storage heating)",
        "open fire": "Question 470: Which of the following do you also use?: Open fires",
        "other solid fuel": "Question 470: Which of the following do you also use?: Other Solid fuel (coal, wood etc)",
        "renewables": "Question 470: Which of the following do you also use?: Renewable (e.g. solar)",
        "other": "Question 470: Which of the following do you also use?: Other",
        "none": "Question 470: Which of the following do you also use?: None of these",
    }

    raw_profiles_edited = raw_profiles[columns.values()]
    return clean_profiles.assign(
        other_non_gas_heating=raw_profiles[column].map(decodings)
    )


@task
def _extract_immersion(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 4701: Which of the following describe how you heat water in your: Electric (immersion)"

    return immersion.assign(
        typical_temp=raw_profiles[column].astype(np.int8).map({0: False, 1: True})
    )


@task
def _extract_electric_shower(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 4701: Which of the following describe how you heat water in your: Electric (instantaneous heater for example your instant shower)"

    return clean_profiles.assign(
        electric_shower=raw_profiles[column].astype(np.int8).map({0: False, 1: True})
    )


@task
def _extract_combi_boiler(
    clean_profiles: pd.DataFrame, raw_profiles: pd.DataFrame,
) -> pd.DataFrame:

    column = "Question 4701: Which of the following describe how you heat water in your: Gas -where there is a small boiler near the tap that gives you instant hot water"

    return clean_profiles.assign(
        typical_temp=raw_profiles[column].replace({" ": np.nan}).map(decodings)
    )


@task
def _save_to_parquet(df: pd.DataFrame, filepath: Path) -> None:

    df.to_parquet(filepath)
