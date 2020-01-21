import os
from pathlib import Path
from shutil import rmtree

import geopandas as gpd  # to read/write spatial data
import pandas as pd
from pipeop import pipes
from funcy import log_durations

import src
from src.helper.logging import create_logger, log_dataframe, log_df
from src.helper.paths import DATA_DIR, PLOT_DIR, ROOT_DIR
from src.helper.plotting import plot_geodf_to_file
from src.helper.save import save_geodataframe

LOGGER = create_logger(caller=__name__)

EDS_FNAME = (
    'CSO_Electoral_Divisions_Ungeneralised__OSi_National_Statistical_Boundaries__2015'
)
PCODES_FNAME = 'map_of_dublin_postcodes'
ED_PCODE_OVERLAP_FNAME = 'ed_pcode_overlap'
OUTPUT_FNAME = 'dublin_eds'


@pipes
def _extract_and_transform_dublin_eds() -> gpd.GeoDataFrame:

    dublin_eds = (
        _load_ireland_eds()
        >> _pull_out_dublin_data
        >> _drop_columns
        >> _rename_columns
        >> _make_strings_lowercase
        >> _reset_index
        >> _rename_electoral_districts_so_compatible_with_cso_map
        >> _convert_object_cols_to_strings_for_merging
    )

    postcodes = _load_dublin_postcodes()

    # Read/write func from file to avoid long run time ...
    path_to_ed_postcode_overlap = DATA_DIR/'interim'/ED_PCODE_OVERLAP_FNAME
    if path_to_ed_postcode_overlap.exists():
        overlap = gpd.read_file(path_to_ed_postcode_overlap)
    else:
        overlap = _get_overlapping_geometries_between_eds_and_pcodes(
            dublin_eds=dublin_eds,
            postcodes=postcodes,
        )
        overlap.to_file(path_to_ed_postcode_overlap)

    ed_postcode_link = (
        overlap
        >> _get_area_of_each_geometry
        >> _get_postcode_with_largest_geometric_area_overlap_for_each_ed
        >> _convert_object_cols_to_strings_for_merging
    )

    return _link_eds_to_postcodes(dublin_eds, ed_postcode_link)


@pipes
def extract_transform_load_dublin_eds() -> None:

    dublin_eds = _extract_and_transform_dublin_eds()
    save_path = (
        ROOT_DIR / 'data' / 'interim' / OUTPUT_FNAME
    )
    dublin_eds.to_file(save_path)

    LOGGER.info(f'Map of Dublin EDs cleaned and saved to {save_path}')

# -----------------------------------------------------------


@log_df(LOGGER)
def _load_ireland_eds() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR / 'data' / 'raw' / EDS_FNAME / f'{EDS_FNAME}.shp'
    )
    return gpd.read_file(path, crs='epsg:4326')


@log_df(LOGGER)
def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / PCODES_FNAME
    )
    return gpd.read_file(path, crs='epsg:4326')


@log_df(LOGGER)
def _pull_out_dublin_data(
    ireland_eds: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    mask = ireland_eds['COUNTY'] == 'DUBLIN'
    return ireland_eds[mask]


@log_df(LOGGER)
def _drop_columns(
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return dublin_eds[['ED_ENGLISH', 'geometry']]


@log_df(LOGGER)
def _rename_columns(
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return dublin_eds.rename({'ED_ENGLISH': 'EDs'}, axis=1)


@log_df(LOGGER)
def _make_strings_lowercase(
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    dublin_eds['EDs'] = (
        dublin_eds['EDs'].str.lower()
    )

    return dublin_eds


@log_df(LOGGER)
def _reset_index(
    dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return dublin_eds.reset_index(drop=True)


@log_df(LOGGER)
def _rename_electoral_districts_so_compatible_with_cso_map(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df['EDs'] = (
        df['EDs']
        .str.replace(pat=r"(')", repl='')  # replace accents with blanks
        .str.replace('saint kevins', 'st. kevins', regex=False)
        .str.replace(
            'dun laoghaire sallynoggin east',
            'dun laoghaire-sallynoggin east',
            regex=False,
        )
        .str.replace(
            'dun laoghaire sallynoggin south',
            'dun laoghaire-sallynoggin south',
            regex=False,
        )
    )

    return df


@log_df(LOGGER)
def _convert_object_cols_to_strings_for_merging(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df[df.select_dtypes(include='object').columns] = (
        df[df.select_dtypes(include='object').columns].astype(str)
    )

    return df


@log_df(LOGGER)
def _get_overlapping_geometries_between_eds_and_pcodes(
    dublin_eds: gpd.GeoDataFrame,
    postcodes: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    '''The result of gpd.overlay identity consists of the surface of df1,
       but with the geometries obtained from overlaying df1 with df2
       '''

    return gpd.overlay(
        df1=dublin_eds,
        df2=postcodes,
        how='identity',
    )


@log_df(LOGGER)
def _get_area_of_each_geometry(
    merged_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return merged_gdf.assign(
        area=(
            merged_gdf['geometry'].apply(
                lambda polygon: polygon.area,
            )
        )
    )


@log_df(LOGGER)
def _get_postcode_with_largest_geometric_area_overlap_for_each_ed(
    merged_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    '''The largest area within each ED represents largest
       overlap with the Postcode geometry.
       '''

    eds_with_largest_overlap = (
        merged_gdf
        .groupby('EDs')['area']
        .nlargest(1)
        .reset_index()
        .set_index('level_1')
    )
    return (
        merged_gdf
        .loc[eds_with_largest_overlap.index, ('EDs', 'Postcodes')]
        .reset_index(drop=True)
    )


@log_df(LOGGER)
@pipes
def _link_eds_to_postcodes(
    dublin_eds: gpd.GeoDataFrame,
    ed_postcode_link: pd.DataFrame,
) -> gpd.GeoDataFrame:
    return (
        dublin_eds
        .merge(ed_postcode_link)
    )
