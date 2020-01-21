import geopandas as gpd
import pandas as pd

from pipeop import pipes
from src.helper.paths import ROOT_DIR, DATA_DIR, PLOT_DIR
from src.helper.logging import create_logger
from src.helper.save import save_geodataframe
from src.helper.plotting import plot_geodf_to_file

LOGGER = create_logger(caller=__name__)


def _load_ireland_admin_areas() -> gpd.GeoDataFrame:

    path = (
        DATA_DIR
        / 'raw'
        / 'Administrative_Areas__OSi_National_Statutory_Boundaries_.geojson'
    )
    return gpd.read_file(path)[['COUNTY', 'geometry']]


def _dissolve_dublin_geometries_into_one(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    gdf = gdf.copy()

    return gdf[gdf['COUNTY'] == 'DUBLIN'].dissolve(by='COUNTY')


@pipes
def extract_and_save_dublin_boundary() -> None:

    dublin_boundary = (
        _load_ireland_admin_areas()
        >> _dissolve_dublin_geometries_into_one
    )

    save_path = DATA_DIR / 'interim' / 'dublin_boundary'
    dublin_boundary.to_file(save_path)

    LOGGER.info(f'Map of Dublin EDs cleaned and saved to {save_path}')
