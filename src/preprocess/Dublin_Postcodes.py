from pathlib import Path
import src
from src.helper.logging import create_logger, log_dataframe
from src.helper.plotting import plot_geodf
from src.helper.save import save_geodataframe

from pipeop import pipes
from typing import Tuple
import geopandas as gpd  # to read/write spatial data
import pandas as pd

from src.helper.paths import ROOT_DIR, DATA_DIR, PLOT_DIR
LOGGER = create_logger(caller=__name__)


def _load_dublin_electoral_districts() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / 'map_of_dublin_eds'
        / 'map_of_dublin_eds.shp'
    )
    return gpd.read_file(path)


def _load_dublin_postcodes_stackexchange() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'raw'
        / 'DublinPostcodes_4326.geojson'
    )
    return gpd.read_file(path, crs='epsg:4326')


def _clean_postcodes_stackexchange(
    dublin: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    these_values = {
        66: 12,
        61: '6w',
        100: 'co. dublin',
        0: 'co. dublin',
    }
    return (
        dublin
        .assign(
            Postcodes=(
                dublin['id']
                .replace(these_values)
                .fillna(0)
            )
        )
    )


def _load_ireland_postcodes() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'raw'
        / 'map_of_Ireland_Routing_Keys'
        / 'map_of_Ireland_Routing_Keys.shp'
    )
    return gpd.read_file(path, crs='epsg:4326')


def _pull_out_dublin_postcodes() -> gpd.GeoDataFrame:

    map_of_ireland = _load_ireland_postcodes()
    map_of_dublin = _load_dublin_electoral_districts()
    return gpd.sjoin(
        left_df=map_of_ireland,
        right_df=map_of_dublin,
    )


def _drop_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.drop(
        labels=['index_right', 'RoutingKey', 'EDs'],
        axis='columns',
    )


def _extract_postcode_numbers(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.assign(
        id=(
            gdf['Postcodes']
            .str.extract(r'(\d+)')
            .fillna(0)
        )
    )


def _rename_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.rename(columns={'Descriptor': 'Postcodes'})


def _set_lowercase(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf['Postcodes'] = gdf['Postcodes'].str.lower()

    return gdf


def _set_postcodes_to_dublin_county(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf['Postcodes'] = gdf['Postcodes'].apply(
        lambda postcode:
        'co. dublin' if 'dublin' not in postcode else postcode
    )

    return gdf


def _dissolve_co_dublin_postcodes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    ''' Merge all co. dublin pcodes into one shape'''
    return (gdf
            .reset_index()
            .dissolve(by='Postcodes')
            )


def _drop_index_col(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.drop(
        labels='index',
        axis='columns',
    )


@pipes
def _data_pipeline() -> gpd.GeoDataFrame:

    return(
        _pull_out_dublin_postcodes()
        >> _drop_columns
        >> _rename_columns
        >> _set_lowercase
        >> _set_postcodes_to_dublin_county
        >> _dissolve_co_dublin_postcodes
        >> _drop_index_col
    )


def load_clean_save_dublin_postcodes() -> None:

    dublin = _data_pipeline()

    save_path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / 'dublin_postcodes'
        / 'dublin_postcodes.shp'
    )
    save_geodataframe(save_path=save_path, gdf=dublin)

    LOGGER.info('Dublin Postcodes pulled out of dublin shapefile')


def annotate_and_plot_geodf(
    gdf: gpd.GeoDataFrame,
    to_annotate: str,
    ax=None,
) -> None:

    gdf['coords'] = gdf['geometry'].apply(
        lambda x: x.representative_point().coords[:])
    gdf['coords'] = [coords[0] for coords in gdf['coords']]

    if ax is None:
        ax = gdf.plot(edgecolor='black')
    else:
        gdf.plot(ax=ax, edgecolor='black')

    gdf.apply(
        lambda x: ax.annotate(
            s=x[to_annotate],
            xy=x.geometry.centroid.coords[0],
            ha='center',
        ), axis=1
    )


def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / 'map_of_dublin_postcodes'
        / 'map_of_dublin_postcodes.shp'
    )
    return gpd.read_file(path, crs='epsg:4326')
