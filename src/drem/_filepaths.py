import codema_drem
from pathlib import Path

# ---------------
# Directory paths
# ---------------

BASE_DIR = Path(__file__).parents[2]
SRC_DIR = Path(__file__).parent
PLOT_DIR = BASE_DIR / "plots"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"


RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
INTERIM_DIR = DATA_DIR / "interim"

FLOW_DIR = BASE_DIR / "plots" / "flows"


# Smart Meter directories
# ***********************

SM_ELEC_DEMANDS_RAW_DIR = RAW_DIR / "smart_meter_electricity"
SM_GAS_DEMANDS_RAW_DIR = RAW_DIR / "smart_meter_gas"
SM_ELEC_DEMANDS_CLEAN_PARQUET_DIR = PROCESSED_DIR / "sm_elec_dask_parquet"
SM_GAS_DEMANDS_CLEAN_PARQUET_DIR = PROCESSED_DIR / "sm_gas_dask_parquet"
SM_ELEC_DEMANDS_CLEAN_CSV_DIR = PROCESSED_DIR / "sm_elec_csvs"
SM_GAS_DEMANDS_CLEAN_CSV_DIR = PROCESSED_DIR / "sm_gas_csvs"

# ---------
# Filepaths
# ---------

# Raw Data
# ********

BER_RAW = RAW_DIR / "BERPublicsearch.txt"
CENSUS_RAW = (
    RAW_DIR / "Census2016-electoral-districts-residential-yearbuilt-dwellingtype.xlsx"
)
CENSUS_PERIOD_BUILT_RAW = (
    RAW_DIR
    / "Census2016 scraped map"
    / "Permanent private households by year built.csv"
)
CENSUS_DWELLING_TYPE_RAW = (
    RAW_DIR
    / "Census2016 scraped map"
    / "Private households by type of accommodation.csv"
)
DUBLIN_EDS_RAW = (
    RAW_DIR
    / "CSO_Electoral_Divisions_Ungeneralised__OSi_National_Statistical_Boundaries__2015"
)
DUBLIN_LAS_RAW = (
    RAW_DIR / "Administrative_Areas__OSi_National_Statutory_Boundaries_.geojson"
)
DUBLIN_POSTCODES_RAW = RAW_DIR / "Dublin postcodes - Shane McGuinness"
MNR_RAW = RAW_DIR / "FOI_Codema_24.1.20.xlsx"
SAS_RAW = (
    RAW_DIR
    / "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
SM_ELEC_RES_PROFILES_RAW = (
    RAW_DIR
    / "smart_meter_electricity"
    / "Smart meters Residential pre-trial survey data.csv"
)
SM_ELEC_SME_PROFILES_RAW = (
    RAW_DIR / "smart_meter_electricity" / "Smart meters SME pre-trial survey data.csv"
)
SM_GAS_RES_PROFILES_RAW = (
    RAW_DIR
    / "smart_meter_gas"
    / "Smart meters Residential pre-trial survey data - Gas.csv"
)
VO_RAW = RAW_DIR / "vo.parquet"


# Interim Data
# ************
MNR_REBECCA = INTERIM_DIR / "(Rebecca) Updated Public Sector M&R.xlsx"


# Processed Data
# **************
BER_CLEAN = PROCESSED_DIR / "ber.parquet"
CENSUS_CLEAN = PROCESSED_DIR / "census2016.parquet"
CENSUS_DWELLING_TYPE_CLEAN = PROCESSED_DIR / "census2016_dwelling_type.parquet"
CENSUS_PERIOD_BUILT_CLEAN = PROCESSED_DIR / "census2016_period_built.parquet"
DUBLIN_BOUNDARY = PROCESSED_DIR / "dublin_boundary"
DUBLIN_EDS_CLEAN = PROCESSED_DIR / "dublin_eds"
DUBLIN_POSTCODES_CLEAN = PROCESSED_DIR / "dublin_postcodes"
MNR_CLEAN = PROCESSED_DIR / "mnr"
SAS_CLEAN = PROCESSED_DIR / "small_areas"
SM_ELEC_RES_PROFILES_CLEAN = PROCESSED_DIR / "sm_elec_res_profiles.parquet"
SM_ELEC_SME_PROFILES_CLEAN = PROCESSED_DIR / "sm_elec_sme_profiles.parquet"
SM_GAS_RES_PROFILES_CLEAN = PROCESSED_DIR / "sm_gas_res_profiles.parquet"
SM_GAS_RES_DEMANDS_CLEAN = PROCESSED_DIR / "sm_gas_res_demands.parquet"
MNR_CLEAN_PARQUET = PROCESSED_DIR / "mnr.parquet"
VO_CLEAN = PROCESSED_DIR / "vo"


# Results
# *******
SYN_RESID_RESULTS = DATA_DIR / "results" / "synthetic_residential_stock.parquet"


# Plots
# *****
DUBLIN_EDS_PLOT = PLOT_DIR / "annotated_dublin_eds"
DUBLIN_POSTCODES_PLOT = PLOT_DIR / "annotated_dublin_postcodes.png"


# Flows
# *****
BER_FLOW = FLOW_DIR / "ber_preprocessing_flow"
CENSUS_FLOW = FLOW_DIR / "census_preprocessing_flow"
DUBLIN_BOUNDARY_FLOW = FLOW_DIR / "dublin_boundary_preprocessing_flow"
DUBLIN_EDS_FLOW = FLOW_DIR / "dublin_eds_preprocessing_flow"
DUBLIN_POSTCODES_FLOW = FLOW_DIR / "dublin_postcodes_preprocessing_flow"
MNR_FLOW = FLOW_DIR / "mnr_preprocessing_flow"
SYN_RESID_FLOW = FLOW_DIR / "synthetic_residential_dev_flow"
VO_FLOW = FLOW_DIR / "vo_preprocessing_flow"
