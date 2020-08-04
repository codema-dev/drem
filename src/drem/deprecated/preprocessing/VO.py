from pathlib import Path

import geopandas as gpd  # to read/write spatial data
import pandas as pd
import recordlinkage
from funcy import log_durations
from icontract import require, ensure
from pipeop import pipes
from prefect import Flow, task
from prefect.engine.executors import LocalDaskExecutor
from prefect.utilities.debug import raise_on_exception

from codema_drem.utilities.geocode import geocode_coordinates
from codema_drem.utilities.flow import run_etl_flow
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

# Intermediate files
GEOCODED_COORDINATES_LOCALHOST = INTERIM_DIR / "vo_localhost_addresses.csv"
GEOCODED_COORDINATES_GMAPS = INTERIM_DIR / "vo_googlemaps_addresses.csv"


# Flow
# ****

@pipes
def vo_etl_flow() -> gpd.GeoDataFrame:

    with Flow("preprocess-VO") as flow:

        vo_clean = (
            _load_vo_raw(VO_RAW)
            >> _set_column_column_names_lowercase
            >> _strip_whitespace_from_column__names
            >> _infer_column_data_types
            >> _drop_faulty_locations
            >> _merge_address_columns_into_one
            >> _set_column_strings_lowercase
            >> _extract_postcodes_from_address_column
            >> _standarise_postcode_names
            >> _convert_to_geodataframe
            >> _pull_out_postcodes_from_address
            >> _pull_out_address_number_if_any
        )

        mnr = _load_mnr_clean(MNR_CLEAN)

        _link_mnr_to_vo(mnr, vo_clean)

    return flow


# NOTE: run_flow(flow_function=vo_etl_flow, viz_path=VO_FLOW)


# Tasks
# *****

def _download_vo_data_to_file(filepath: Path) -> None:

    la_urls = [
        "https://api.valoff.ie/api/Property/GetProperties?Fields=*&LocalAuthority=DUN%20LAOGHAIRE%20RATHDOWN%20CO%20CO&CategorySelected=OFFICE%2CFUEL%2FDEPOT%2CLEISURE%2CINDUSTRIAL%20USES%2CHEALTH%2CHOSPITALITY%2CMINERALS%2CMISCELLANEOUS%2CRETAIL%20(SHOPS)%2CUTILITY%2CRETAIL%20(WAREHOUSE)%2CNO%20CATEGORY%20SELECTED%2CCENTRAL%20VALUATION%20LIST%2CCHECK%20CATEGORY%2CNON-LIST%2CNON-LIST%20EXEMPT&Format=csv&Download=true",
        "https://api.valoff.ie/api/Property/GetProperties?Fields=*&LocalAuthority=DUBLIN%20CITY%20COUNCIL&CategorySelected=LEISURE%2CINDUSTRIAL%20USES%2CHEALTH%2CHOSPITALITY%2CMINERALS%2CMISCELLANEOUS%2CRETAIL%20(SHOPS)%2CUTILITY%2CRETAIL%20(WAREHOUSE)%2CNO%20CATEGORY%20SELECTED%2CCENTRAL%20VALUATION%20LIST%2CCHECK%20CATEGORY%2CNON-LIST%2CNON-LIST%20EXEMPT&Format=csv&Download=true",
        "https://api.valoff.ie/api/Property/GetProperties?Fields=*&LocalAuthority=SOUTH%20DUBLIN%20COUNTY%20COUNCIL&CategorySelected=LEISURE%2CINDUSTRIAL%20USES%2CHEALTH%2CHOSPITALITY%2CMINERALS%2CMISCELLANEOUS%2CRETAIL%20(SHOPS)%2CUTILITY%2CRETAIL%20(WAREHOUSE)%2CNO%20CATEGORY%20SELECTED%2CCENTRAL%20VALUATION%20LIST%2CCHECK%20CATEGORY%2CNON-LIST%2CNON-LIST%20EXEMPT&Format=csv&Download=true",
        "https://api.valoff.ie/api/Property/GetProperties?Fields=*&LocalAuthority=FINGAL%20COUNTY%20COUNCIL&CategorySelected=LEISURE%2CINDUSTRIAL%20USES%2CHEALTH%2CHOSPITALITY%2CMINERALS%2CMISCELLANEOUS%2CRETAIL%20(SHOPS)%2CUTILITY%2CRETAIL%20(WAREHOUSE)%2CNO%20CATEGORY%20SELECTED%2CCENTRAL%20VALUATION%20LIST%2CCHECK%20CATEGORY%2CNON-LIST%2CNON-LIST%20EXEMPT&Format=csv&Download=true",
    ]

    vo_dfs = [pd.read_csv(url) for url in la_urls]
    vo = pd.concat(vo_dfs)

    vo.to_parquet(VO_RAW)


@task
def _load_vo_raw(filepath: Path) -> pd.DataFrame:

<<<<<<< HEAD
    vo_path = BASE_DIR / "data" / "interim" / f"{RAW_VO_FNAME}"
    vo = pd.read_pickle(vo_path)
=======
    return pd.read_parquet(filepath)
>>>>>>> merge_vo_mnr


@task
def _set_column_column_names_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.rename(columns=str.lower)


@task
def _strip_whitespace_from_column__names(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.rename(columns=str.strip)


@task
def _infer_column_data_types(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.convert_dtypes()


@task
def _drop_faulty_locations(vo: pd.DataFrame) -> pd.DataFrame:
    """Drops all locations with an X ITM < 70,000 which eliminates all
    buildings with invalid coordinates

    Parameters
    ----------
    vo : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    # TODO: geolocate all faulty locations using their addresses...
    return vo.loc[vo["x itm"] > 700000]


@task
def _merge_address_columns_into_one(vo: pd.DataFrame) -> pd.DataFrame:

    vo["address"] = (
        + vo["address 1"]
        + " "
        + vo["address 2"]
        + " 
        + vo["address 3"]
        + " "
        + vo["address 4"]
        + " "
        + vo["address 5"]
    )

    return vo.drop(
        columns=["address 1", "address 2", "address 3", "address 4", "address 5",]
    )


@task
def _set_column_strings_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.assign(
        address=vo["address"].str.lower()
    )


@task
def _extract_postcodes_from_address_col(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(
        postcodes=(
            vo["address"].str.replace(pat=r"\b(dublin.+)\b", repl="", regex=True,)
        )
    )
    # NOTE: (dublin[ ]?[\d+]?)
    # NOTE - if just dublin captured can assume pcode is co. dub


@task
def _standarise_postcode_names(vo: pd.DataFrame) -> pd.DataFrame:

    import ipdb
    ipdb.set_trace()

    return 


@task
def _convert_to_geodataframe(vo: pd.DataFrame,) -> gpd.GeoDataFrame:

    locations = gpd.points_from_xy(x=vo["x itm"], y=vo["y itm"])
    return gpd.GeoDataFrame(vo, geometry=locations, crs="epsg:2157").to_crs("epsg:4326")


@task
def _pull_out_address_number_if_any(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(address_number=vo["address"].str.extract(pat=r"(\d+/?\w*)"))



@task
def _create_gps_coordinates_column(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return vo.assign(
        coordinates= vo.geometry.y.astype(str) + ", " + vo.geometry.x.astype(str)
    )


@task
@require(lambda vo: "coordinates" in vo.columns)
def _create_alternative_address_column_via_geolocation(
    vo: pd.DataFrame,
    geocoded_coordinates_filepath: Path,
    geocoding_platform: str = "localhost",
) -> pd.DataFrame:

    if not geocoded_coordinates_filepath.exists():
        geocode_coordinates(
            coordinates=vo["coordinates"],
            geocoding_platform=geocoding_platform,
            save_path=geocoded_coordinates_filepath,
        )

    return vo.assign(
        geocoded_address=pd.read_csv(
            filepath_or_buffer=geocoded_coordinates_filepath, usecols=["address"],
        )


@task
@require(lambda vo: "geocoded_address" in vo.columns)
def _make_geocoded_addresses_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.assign(
        geocoded_address=vo["geocoded_address"].str.lower(),
    )



@task
def _pull_out_postcodes_from_geolocated_address(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(
        postcode=vo["geocoded_address"].str.extract(pat=r"(dublin[ ]?[\d+]?)")
    )
    # NOTE: if just dublin captured can assume pcode is co. dub


@task
def _drop_columns(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    keep_columns = [
        "address",
        "geocoded_address",
        "address_number",
        "postcode",
        "area",
        "property number",
        "geometry",
    ]
    return vo[keep_columns]



@task
def _load_mnr_clean(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def _load_dublin_eds(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def _plot_vo_mnr_dublin(
    vo: gpd.GeoDataFrame, mnr: gpd.GeoDataFrame, map_of_dublin: gpd.GeoDataFrame,
) -> None:

    fig, ax = plt.subplots(sharex=True, sharey=True, figsize=(20, 16))
    plot_path = PLOT_DIR / "vo_mnr_dublin.png"

    map_of_dublin.plot(ax=ax, color="white", edgecolor="black")
    vo.plot(ax=ax, color="red", markersize=0.1)
    # mnr.plot(ax=ax, color='blue', markersize=0.1)
    mnr.plot(ax=ax, color="blue")

    fig.savefig(plot_path)


@task
def _link_mnr_to_vo(mnr: gpd.GeoDataFrame, vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    import ipdb

    ipdb.set_trace()
    indexer = recordlinkage.Index()
    indexer.block
