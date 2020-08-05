from typing import Tuple
from pathlib import Path

import geopandas as gpd  # to read/write spatial data
import numpy as np
import pandas as pd
from icontract import require, ensure
from pipeop import pipes
from prefect import Flow, task
from tqdm import tqdm
from unidecode import unidecode

from codema_drem.utilities.flow import run_etl_flow
from codema_drem.utilities.geocode import geocode_addresses
from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
<<<<<<< HEAD
    BASE_DIR,
=======
    INTERIM_DIR,
>>>>>>> merge_vo_mnr
    MNR_REBECCA,
    MNR_RAW,
    MNR_CLEAN_PARQUET,
    MNR_FLOW,
)

GEOCODING_PLATFORM = "localhost"

# Intermediate files
GEOCODED_COORDINATES_LOCALHOST = INTERIM_DIR / "mnr_localhost_addresses.csv"
GEOCODED_COORDINATES_GMAPS = INTERIM_DIR / "mnr_googlemaps_addresses.csv"


# Flows
# *****


@pipes
def mnr_etl_flow() -> gpd.GeoDataFrame:

    with Flow("preprocess-mnr") as flow:

        mnr_clean = (
            _load_mnr_rebecca(MNR_REBECCA)
            >> _set_column_column_names_lowercase
            >> _strip_whitespace_from_column__names
            >> _rename_columns
            >> _merge_columns_into_address
            >> _set_column_strings_lowercase
            >> _remove_special_characters_from_address_column
            >> _remove_commas_from_address_column
            >> _standardise_postcodes
            >> _drop_columns
            # >> _convert_numeric_columns_to_floats
        )

        _save_to_parquet(mnr_clean, MNR_CLEAN_PARQUET)

    return flow


# NOTE: run_flow(flow_function=mnr_etl_flow, viz_path=MNR_FLOW)


# Tasks
# *****


# ! ------------------------------------------------
# ! On hold until fuzzy merging algorithm written:


@task
def _load_mnr_raw(filepath: Path) -> pd.DataFrame:

<<<<<<< HEAD
    mnr_path = BASE_DIR / "data" / "raw" / INPUT_NAME_RAW
=======
>>>>>>> merge_vo_mnr
    mnr = pd.read_excel(
        filepath, sheet_name=["MPRN_data", "GPRN_data"], engine="openpyxl"
    )
    mnr_mprn, mnr_gprn = mnr["MPRN_data"], mnr["GPRN_data"]

    return (mnr_mprn, mnr_gprn)


@task
def _filter_out_non2018_data(
    mnr: Tuple[pd.DataFrame, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    mnr_mprn = mnr[0]
    mnr_gprn = mnr[1]

    year = 2018
    mask_mprn = mnr_mprn["Year"] == year
    mask_gprn = mnr_gprn["Year"] == year
    return (mnr_mprn[mask_mprn], mnr_gprn[mask_gprn])


@task
def _merge_mprn_and_gprn(mnr: Tuple[pd.DataFrame, pd.DataFrame],) -> pd.DataFrame:

    raise NotImplementedError("Requires fuzzy merge...")

    mnr_mprn = mnr[0]
    mnr_gprn = mnr[1]

    return pd.merge(
        left=mnr_mprn,
        right=mnr_gprn,
        left_on=["pb name", "Location", "Category", "County"],
        right_on=["pb name", "Location", "Category", "County"],
    )


# ! ------------------------------------------------


# Using Rebecca's M&R Data
# ------------------------


@task
def _load_mnr_rebecca(filepath: Path) -> pd.DataFrame:

    return pd.read_excel(filepath, sheet_name="Total Energy", engine="openpyxl")


@task
def _set_column_column_names_lowercase(mnr: pd.DataFrame) -> pd.DataFrame:

    return mnr.rename(columns=str.lower)


@task
def _strip_whitespace_from_column__names(mnr: pd.DataFrame) -> pd.DataFrame:

    return mnr.rename(columns=str.strip)


@task
def _rename_columns(mnr: pd.DataFrame) -> pd.DataFrame:

    return mnr.rename(
        columns={
            "county": "postcodes",
            "electricty (kwh)": "annual_electricity",
            "gas (kwh)": "annual_gas",
        }
    )


@task
def _infer_column_data_types(mnr: pd.DataFrame) -> pd.DataFrame:

    return mnr.convert_dtypes()


@task
def _merge_columns_into_address(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(
        address=mnr["pb name"].astype(str) + " " + mnr["location"].astype(str)
    )


@task
def _set_column_strings_lowercase(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(
        address=mnr["address"].str.lower(), postcodes=mnr["postcodes"].str.lower()
    )


@task
def _remove_special_characters_from_address_column(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(address=mnr["address"].apply(unidecode))


@task
def _remove_commas_from_address_column(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(address=mnr["address"].str.replace(",", ""))


@task
def _remove_all_duplicate_words_in_address_col(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(
        address=(
            mnr["address"].str.replace(
                pat=r"\b(\w+)\b(?=.*?\b\1\b)", repl="", regex=True,
            )
        )
    )


@task
def _pull_out_address_number_if_any(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(address_number=(mnr["address"].str.extract(pat=r"(\d+)")))


@task
@require(lambda mnr: "postcodes" in mnr.columns)
@require(lambda mnr: mnr["postcodes"].str.islower().all())
def _standardise_postcodes(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.assign(
        postcodes=mnr["postcodes"].replace({"dublin (county)": "co. dublin"})
    )


@task
def _drop_columns(mnr: pd.DataFrame,) -> pd.DataFrame:

    return mnr.drop(columns=["pb name", "location", "consumption category"])


@task
def _convert_numeric_columns_to_floats(mnr: pd.DataFrame,) -> pd.DataFrame:
    """Parquet breaks with int64 right now so need np.int or np.float ...

    Parameters
    ----------
    mnr : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    return mnr.assign(
        id=mnr["id"].astype(np.uint16),
        annual_electricity=mnr["annual_electricity"].astype(np.float16),
        annual_gas=mnr["annual_gas"].astype(np.float16),
    )


@task
def _save_to_parquet(mnr: pd.DataFrame, filepath: Path) -> None:

    mnr.to_parquet(filepath)


# Geolocation
# -----------


@task
def _geocode_address_column(
    mnr: pd.DataFrame, geocoded_addresses_file_path: Path, geocoding_platform: str,
) -> pd.DataFrame:

    if not geocoded_addresses_file.exists():
        geocode_addresses(
            geocoding_platform=geocoding_platform,
            addresses=mnr["address"],
            save_path=geocoded_addresses_file,
        )

    mnr[["latitude", "longitude"]] = pd.read_csv(
        filepath_or_buffer=geocoded_addresses_file, usecols=["latitude", "longitude"]
    )

    return mnr


@task
def _convert_dataframe_to_a_geodataframe(mnr: pd.DataFrame) -> (gpd.GeoDataFrame):

    locations = gpd.points_from_xy(x=mnr["latitude"], y=mnr["longitude"])
    return gpd.GeoDataFrame(mnr, geometry=locations, crs="epsg:4326")


@task
def _manually_fix_non_dublin_googlemaps_addresses(
    mnr: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    outliers = {
        "AHEAD": (53.2955759, -6.1878618),
        "daa plc": (53.42931949, -6.2462264),
        "Irish Greyhound Board / Bord na gCon": (53.3240619, -6.277817),
        "CLOCHAR SAN DOMINIC": (53.2859253, -6.3572716),
    }

    for key, value in outliers.items():
        mnr.loc[mnr["pb name"] == key, ("latitude", "longitude")] = value

    # NCSE is in Meath & HSE returns no results so drop em:
    mask = (
        mnr["pb name"]
        == "National Council for Special Education" | mnr["pb name"]
        == "HSE"
    )
    return mnr.drop(mnr.loc[mask].index)


@task
def _drop_unsuccessful_geolocations(mnr_raw: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    mask = (mnr_raw["latitude"].isnull()) | (mnr_raw["longitude"].isnull())
    return mnr_raw[~mask]
