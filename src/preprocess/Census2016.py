from src.helper.paths import ROOT_DIR, DATA_DIR, PLOT_DIR
from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from toolz.functoolz import pipe
from pipeop import pipes
import geopandas as gpd
from unidecode import unidecode

import src
from src.helper.logging import (
    create_logger,
    log_dataframe,
    log_df,
)

np.seterr(all='raise')  # force numpy runtime errors to stop code

CENSUS_FILENAME = (
    'Census2016-electoral-districts-residential-yearbuilt-dwellingtype.xlsx'
)
PCODE_FNAME = 'dublin_postcodes'
OUTPUT_FNAME = 'census2016_clean.pkl'
LOGGER = create_logger(caller=__name__)

# TODO: deduplicate eds & merge duplicates
# should have 318 eds not 322
# duplicates are:
# 'dun laoghaire-sallynoggin east',
# 'dun laoghaire-sallynoggin south',
# 'rathfarnham-st. endas',
# 'st. kevins'


@pipes
def _extract_and_transform_census() -> pd.DataFrame:

    return (
        _load_census2016()
        >> _drop_empty_rows
        >> _replace_nans_with_zero
        >> _replace_less_than_x_values
        >> _replace_less_than_6_with_number
        >> _distribute_not_stated_columns
        >> _drop_not_stated_columns
        >> _stack_columns_in_index
        >> _rename_dataframe
        >> _remove_numbers_from_eds

        # change census so compatible with ber for merge
        >> _drop_all_years_rows_in_period_built
        >> _change_dwelling_type_names_to_same_as_ber
        >> _set_lowercase_strings
        >> _rename_electoral_districts_so_compatible_with_cso_map
        >> _convert_object_cols_to_strings_for_merging

        # >> _set_census_index
    )


def extract_transform_load_census() -> pd.DataFrame:

    df_clean = _extract_and_transform_census()
    save_path = ROOT_DIR/'data'/'interim'/OUTPUT_FNAME
    df_clean.to_pickle(save_path)

    LOGGER.info(f'Census2016 cleaned and saved to {save_path}')

# -----------------------------------------------------------


@log_df(logger=LOGGER)
def _load_census2016() -> pd.DataFrame:

    path_to_data = ROOT_DIR / 'data' / 'raw' / CENSUS_FILENAME
    df = pd.read_excel(
        path_to_data,
        header=[1, 2],
        index_col=0,
        skiprows=[3],
        engine='openpyxl',
    )

    df.index.name = 'EDs'
    df.name = 'Census2016'

    return df


@log_df(logger=LOGGER)
def _load_dublin_postcodes() -> gpd.GeoDataFrame:

    path = (
        ROOT_DIR
        / 'data'
        / 'interim'
        / PCODE_FNAME
    )
    return gpd.read_file(path)


@log_df(logger=LOGGER)
def _get_all_of_dublin_total(df: pd.DataFrame) -> pd.DataFrame:

    LOGGER.info('Loading Census2016 Dublin total data ...')

    path_to_raw_data = path.get(CENSUS_FILENAME)
    dublin_total = pd.read_excel(
        path_to_raw_data,
        header=[1, 2],
        skiprows=0,
        nrows=1,
        engine='openpyxl'
    )

    dublin_total.index.name = 'EDs'

    return dublin_total


@log_df(logger=LOGGER)
def _drop_empty_rows(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return df.dropna(how='all')


@log_df(logger=LOGGER, rows=30)
def _replace_nans_with_zero(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return df.fillna(0)


def _replace_less_than_x_values(
    df_raw: pd.DataFrame
) -> pd.DataFrame:

    df = df_raw.copy()

    idx = pd.IndexSlice

    # Replace all <X in Not stated columns with X
    # & all <6 in All years columns with 5
    df.loc[:, idx[:, ('Not stated', 'All Years')]] = (
        df_raw.loc[:, idx[:, ('Not stated', 'All Years')]]
        .replace({r'<': ''}, regex=True)
        .dropna()
        .astype(np.int32)
    )

    # Replace all <6 with zeros
    df.loc[:, :] = (
        df.loc[:, :]
        .replace({r'<6': 0}, regex=True)
        .dropna()
        .astype(np.int32)
    )

    log_dataframe(
        df,
        LOGGER,
        'All <X values replaced with X & all <6 with 0',
    )
    return df


def _replace_less_than_6_with_number(df: pd.DataFrame):

    dwelling_types = [
        'All Households',
        'Detached house',
        'Semi-detached house',
        'Terraced house',
        'All Apartments and Bed-sits',
        'Not stated',
    ]

    idx = pd.IndexSlice

    # replace <6 with any of 0,1,2,3,4,5 depending on row total
    for replacing in range(5):
        for dwelling_type in dwelling_types:

            df_view = df[dwelling_type]

            # grab out only rows where SUM(columns) < All Years
            mask = (
                df_view['All Years']
                > df_view.loc[:, df_view.columns != 'All Years'].sum(axis=1)
            )

            # and add 1 to all cells in row that are <5
            df.loc[mask, idx[dwelling_type, :]] = (
                df.loc[mask, idx[dwelling_type, :]].replace(
                    to_replace={replacing: replacing+1},
                )
            )

            # DEBUGGING: prints out data as transformed to a log file
            log_dataframe(
                df.loc[mask, idx[dwelling_type, :]],
                LOGGER,
                f'replacing {replacing} with {replacing+1}',
                max_columns=20,
            )

    log_dataframe(
        df,
        LOGGER,
        'All <6 values replaced with appropriate number',
    )
    return df


def _distribute_not_stated_column(
    old_column: np.array,
    not_stated_column: np.array,
    row_total_column: np.array
) -> np.array:
    ''' For row indexed by building period 2001-2005 there are:
        - 114 Apartments
        - 1 Semi-detached house
        - 1 detached house
        - 1 Not stated

        Therefore, the one Not stated value is most likely to be an apartment.
        To capture this in an equation; it will be redistributed in proportion
        to its relative weight so:

            New Apartment value = Old Apartment value + round(1 x 114/117)

        The round function rounds up the fraction to the nearest integer.
        In this case the equation will return 1 and so the New Apartment
        value is 115

        '''

    not_stated_column_distributed = (
        old_column
        + np.round(
            not_stated_column*(old_column/row_total_column)
        )
    )

    # Where Not Stated == 0 keep the old value else distribute it!
    not_stated_column_applied = np.where(
        not_stated_column > 0,
        not_stated_column_distributed,
        not_stated_column,
    )

    return not_stated_column_applied


def _distribute_not_stated_columns(
    df_raw: pd.DataFrame,
) -> pd.DataFrame:

    dwelling_types = [
        'All Households',
        'Detached house',
        'Semi-detached house',
        'Terraced house',
        'All Apartments and Bed-sits'
    ]

    periods_built = [
        'before 1919',
        '1919 - 1945',
        '1946 - 1960',
        '1961 - 1970',
        '1971 - 1980',
        '1981 - 1990',
        '1991 - 2000',
        '2001 - 2010',
        '2011 or later'
    ]

    df_redistributed = df_raw.copy()

    for dwelling_type in dwelling_types:
        for period_built in periods_built:

            # 1. distribute 'Not stated' period built
            df_redistributed.loc[:, (dwelling_type, period_built)] = (
                _distribute_not_stated_column(
                    df_raw[dwelling_type][period_built].values,
                    df_raw[dwelling_type]['Not stated'].values,
                    df_raw[dwelling_type]['All Years'].values,
                )
            )

            # 2. distribute 'Not stated' dwelling type
            df_redistributed.loc[:, (dwelling_type, period_built)] = (
                _distribute_not_stated_column(
                    df_redistributed[dwelling_type][period_built].values,
                    df_redistributed['Not stated'][period_built].values,
                    df_redistributed['All Households']['All Years'].values,
                )
            )

    log_dataframe(
        df_redistributed,
        LOGGER,
        'Not stated columns redistributed',
    )
    return df_redistributed


def _drop_not_stated_columns(df: pd.DataFrame) -> pd.DataFrame:

    df.drop(
        columns=['All Households', 'Not stated'],
        axis='columns',
        level=0,
        inplace=True,
    )

    df.drop(
        columns=['Not stated'],
        axis='columns',
        level=1,
        inplace=True,
    )

    log_dataframe(
        df=df,
        logger=LOGGER,
        name='Not stated columns dropped',
    )
    return df


def _stack_columns_in_index(df: pd.DataFrame) -> pd.DataFrame:

    df = df.stack(0).stack().to_frame()
    log_dataframe(
        df=df,
        logger=LOGGER,
        name='DataFrame columns stacked in index',
    )
    return df


def _rename_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df.index.names = ['EDs', 'Dwelling_Type', 'Period_Built']
    df.name = 'Census2016'
    df = df.rename({0: 'Total_HH'}, axis='columns')

    LOGGER.debug('Indexes, columns and dataframe renamed')
    return df


def _remove_numbers_from_eds(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.reset_index()
    df['EDs'] = df['EDs'].str.extract(r'\d+(.+)')
    return df


def _drop_all_years_rows_in_period_built(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return df.loc[df['Period_Built'] != 'All Years']


def _change_dwelling_type_names_to_same_as_ber(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df['Dwelling_Type'] = df['Dwelling_Type'].replace(
        {'All Apartments and Bed-sits': 'apartments'}
    )

    return df


def _set_lowercase_strings(
    df: pd.DataFrame,
) -> pd.DataFrame:

    mask_strings = df.dtypes == np.object
    df.loc[:, mask_strings] = (
        df.loc[:, mask_strings]
        .stack()
        .str.lower()
        .unstack()
    )

    return df


@log_df(LOGGER)
def _rename_electoral_districts_so_compatible_with_cso_map(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df['EDs'] = (
        df['EDs']
        .apply(unidecode)  # replace fadas with letters
        .str.replace(pat=r"[']", repl='')  # replace accents with blanks
        .str.replace(
            'foxrock-deans grange',
            'foxrock-deansgrange',
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


def _set_census_index(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return df.set_index(['EDs', 'Dwelling_Type', 'Period_Built'])


# if __name__ == '__main__':

#     print('\nCall funcs manually to execute!')
