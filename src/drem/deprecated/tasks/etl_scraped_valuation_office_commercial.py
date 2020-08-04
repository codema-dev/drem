from pathlib import Path

import geopandas as gpd  # to read/write spatial data
import pandas as pd
import recordlinkage
from funcy import log_durations
from icontract import require, ensure
from pipeop import pipes
from prefect import Flow, task
from prefect.engine.signals import SKIP
from prefect.engine.executors import LocalDaskExecutor
from prefect.utilities.debug import raise_on_exception

from codema_drem.utilities.geocode import geocode_coordinates
from codema_drem.scrape_valuation_office.scrape_valuation_office import (
    scrape_valuation_office_data_to_parquet,
)


@task
def download_valuation_office_data_to_file(filepath: Path) -> None:

    if not filepath.exists():
        scrape_valuation_office_data_to_parquet(filepath)

    else:
        raise SKIP("Valuation Office data has already been downloaded")


@task
def load_parquet_to_dataframe(filepath: Path) -> pd.DataFrame:

    return pd.read_parquet(filepath)


@task
def save_dataframe_to_parquet(df: pd.DataFrame, filepath: Path) -> None:

    df.to_parquet(filepath)


@task
def set_column_names_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.rename(columns=str.lower)


@task
def strip_whitespace_from_column__names(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.rename(columns=str.strip)


@task
def infer_column_data_types(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.convert_dtypes()


@task
def drop_locations_outside_dublin(vo: pd.DataFrame) -> pd.DataFrame:
    """Drops all locations with an X ITM < 70,000 which eliminates all
    buildings with invalid coordinates

    Parameters
    ----------
    vo : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    return vo.loc[vo["x itm"] > 700000]


@task
def merge_address_columns_into_one(vo: pd.DataFrame) -> pd.DataFrame:

    vo["address"] = (
        +vo["address 1"]
        + " "
        + vo["address 2"]
        + " "
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
def set_column_strings_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.assign(address=vo["address"].str.lower())


@task
def extract_postcodes_from_address_column(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(
        postcodes=(
            vo["address"].str.replace(pat=r"\b(dublin.+)\b", repl="", regex=True,)
        )
    )
    # NOTE: (dublin[ ]?[\d+]?)
    # NOTE - if just dublin captured can assume pcode is co. dub


@task
def standarise_postcode_names(vo: pd.DataFrame) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    return


@task
def convert_to_geodataframe(vo: pd.DataFrame,) -> gpd.GeoDataFrame:

    locations = gpd.points_from_xy(x=vo["x itm"], y=vo["y itm"])
    return gpd.GeoDataFrame(vo, geometry=locations, crs="epsg:2157").to_crs("epsg:4326")


@task
def extract_address_number(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(address_number=vo["address"].str.extract(pat=r"(\d+/?\w*)"))


@task
def create_gps_coordinates_column(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return vo.assign(
        coordinates=vo.geometry.y.astype(str) + ", " + vo.geometry.x.astype(str)
    )


@task
@require(lambda vo: "coordinates" in vo.columns)
def create_alternative_address_column_via_geolocation(
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
    )


@task
@require(lambda vo: "geocoded_address" in vo.columns)
def make_geocoded_addresses_lowercase(vo: pd.DataFrame) -> pd.DataFrame:

    return vo.assign(geocoded_address=vo["geocoded_address"].str.lower(),)


@task
def pull_out_postcodes_from_geolocated_address(vo: pd.DataFrame,) -> pd.DataFrame:

    return vo.assign(
        postcode=vo["geocoded_address"].str.extract(pat=r"(dublin[ ]?[\d+]?)")
    )
    # NOTE: if just dublin captured can assume pcode is co. dub


@task
def drop_columns(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

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
def load_mnr_clean(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def load_dublin_eds(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)


@task
def plot_vo_mnr_dublin(
    vo: gpd.GeoDataFrame, mnr: gpd.GeoDataFrame, map_of_dublin: gpd.GeoDataFrame,
) -> None:

    fig, ax = plt.subplots(sharex=True, sharey=True, figsize=(20, 16))
    plot_path = PLOT_DIR / "vo_mnr_dublin.png"

    map_of_dublin.plot(ax=ax, color="white", edgecolor="black")
    vo.plot(ax=ax, color="red", markersize=0.1)
    # mnr.plot(ax=ax, color='blue', markersize=0.1)
    mnr.plot(ax=ax, color="blue")

    fig.savefig(plot_path)


# @task
# def link_mnr_to_vo(mnr: gpd.GeoDataFrame, vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

#     import ipdb

#     ipdb.set_trace()
#     indexer = recordlinkage.Index()
#     indexer.block
