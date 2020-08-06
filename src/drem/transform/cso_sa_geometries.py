from pathlib import Path

import icontract
import geopandas as gpd
import numpy as np
from prefect import Flow, task


@icontract.ensure(lambda result: len(result["COUNTYNAME"].unique()) == 4)
def _read_only_dublin_local_authorities(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)[
        lambda x: np.isin(
            x["COUNTYNAME"],
            ["DÃºn Laoghaire-Rathdown", "Fingal", "South Dublin", "Dublin City"],
        )
    ]


def _set_coordinate_reference_system_to_wgs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf.crs = "epsg:4326"

    return gdf


@task(name="Transform Small Area Geometries")
def cso_sa_geometries(filepath: Path) -> gpd.GeoDataFrame:

    sa_geometries_raw = _read_only_dublin_local_authorities(filepath)

    return (
        sa_geometries_raw.pipe(_set_coordinate_reference_system_to_wgs)
        .loc[:, ["SMALL_AREA", "EDNAME", "geometry"]]
        .rename(columns={"SMALL_AREA": "sas", "EDNAME": "eds"})
        .assign(eds=lambda x: x["eds"].str.lower())
    )
