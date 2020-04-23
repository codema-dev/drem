from pipeop import pipes
from prefect import Flow

from codema_drem.utilities.flow import run_flow
from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
    INTERIM_DIR,
    VO_RAW,
    VO_CLEAN,
    VO_FLOW,
    DUBLIN_EDS_CLEAN,
    MNR_CLEAN,
)
from codema_drem.tasks.etl_scraped_valuation_office_commercial import (
    download_valuation_office_data_to_file,
    load_parquet_to_dataframe,
    save_dataframe_to_parquet,
    set_column_names_lowercase,
    strip_whitespace_from_column__names,
    drop_locations_outside_dublin,
    merge_address_columns_into_one,
    set_column_strings_lowercase,
    extract_postcodes_from_address_column,
    standarise_postcode_names,
    convert_to_geodataframe,
    extract_address_number,
    load_mnr_clean,
    # link_mnr_to_vo,
)

# Intermediate files
GEOCODED_COORDINATES_LOCALHOST = INTERIM_DIR / "vo_localhost_addresses.csv"
GEOCODED_COORDINATES_GMAPS = INTERIM_DIR / "vo_googlemaps_addresses.csv"


@pipes
def flow_valuation_office_etl() -> Flow:

    with Flow("preprocess-VO") as flow:

        data_downloaded = download_valuation_office_data_to_file(VO_RAW)
        load_vo = load_parquet_to_dataframe(VO_RAW, upstream_tasks=[data_downloaded])

        clean_vo = (
            set_column_names_lowercase(load_vo)
            >> strip_whitespace_from_column__names
            >> drop_locations_outside_dublin
            >> merge_address_columns_into_one
            >> set_column_strings_lowercase
            >> extract_postcodes_from_address_column
            # >> standarise_postcode_names
            >> extract_address_number
        )
        save_dataframe_to_parquet(clean_vo, VO_CLEAN)

        # mnr = load_mnr_clean(MNR_CLEAN)

        # _link_mnr_to_vo(mnr, vo_clean)

    return flow
