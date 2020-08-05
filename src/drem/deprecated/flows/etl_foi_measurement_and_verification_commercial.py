from pipeop import pipes
from prefect import Flow

from codema_drem.utilities.flow import run_flow
from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
    INTERIM_DIR,
    MNR_REBECCA,
    MNR_RAW,
    MNR_CLEAN_PARQUET,
    MNR_FLOW,
)
from codema_drem.tasks.etl_foi_mnr_2019 import (
    load_mnr_rebecca,
    set_column_names_lowercase,
    strip_whitespace_from_column__names,
    rename_columns,
    merge_columns_into_address,
    set_column_strings_lowercase,
    remove_special_characters_from_address_column,
    remove_commas_from_address_column,
    standardise_postcodes,
    keep_columns,
    save_to_parquet,
)

GEOCODING_PLATFORM = "localhost"

# Intermediate files
GEOCODED_COORDINATES_LOCALHOST = INTERIM_DIR / "mnr_localhost_addresses.csv"
GEOCODED_COORDINATES_GMAPS = INTERIM_DIR / "mnr_googlemaps_addresses.csv"


@pipes
def flow_mnr_etl() -> Flow:

    with Flow("preprocess-mnr") as flow:

        mnr_clean = (
            load_mnr_rebecca(MNR_REBECCA)
            >> set_column_names_lowercase
            >> strip_whitespace_from_column__names
            >> rename_columns
            >> merge_columns_into_address
            >> set_column_strings_lowercase
            >> remove_special_characters_from_address_column
            >> remove_commas_from_address_column
            >> standardise_postcodes
            >> keep_columns
        )

        save_to_parquet(mnr_clean, MNR_CLEAN_PARQUET)

    return flow
