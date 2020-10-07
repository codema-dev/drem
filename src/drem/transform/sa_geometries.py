from pathlib import Path

import geopandas as gpd
import icontract
import numpy as np

from prefect import task
from unidecode import unidecode


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def extract_dublin_local_authorities(geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Extract Dublin Local Authorities from Ireland Small Area Geometries Data.

    Args:
        geometries (gpd.GeoDataFrame): Ireland Small Area Geometries

    Returns:
        gpd.GeoDataFrame: Dublin Small Area Geometries
    """
    return geometries.assign(COUNTYNAME=lambda x: x["COUNTYNAME"].apply(unidecode))[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["Dun Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


@task(name="Transform Small Area Geometries")
def transform_sa_geometries(dirpath: Path, filename: str) -> gpd.GeoDataFrame:
    """Transform Small Area geometries.

    Args:
        dirpath (Path): Directory where data is stored
        filename (str): Name of data

    Returns:
        gpd.GeoDataFrame: Clean Small Area Geometries
    """
    filepath = dirpath / f"{filename}.parquet"
    return (
        gpd.read_parquet(filepath)
        .pipe(extract_dublin_local_authorities)
        .to_crs("epsg:4326")
        .loc[:, ["SMALL_AREA", "geometry"]]
        .rename(columns={"SMALL_AREA": "small_area"})
    )
