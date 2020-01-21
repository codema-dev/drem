import pandas as pd
import numpy as np
from pathlib import Path

from pipeop import pipes

# import custom functions
import src
from src.helper.logging import create_logger

from src.helper.paths import ROOT_DIR, DATA_DIR, PLOT_DIR
LOGGER = create_logger(caller=__name__)

INPUT_NAME = 'BERPublicsearch'
OUTPUT_NAME = 'map_of_dublin_postcodes'

np.seterr(all='raise')  # force numpy runtime errors to stop code


@pipes
def load_clean_save_ber() -> None:

    ber = (
        _load_ber_pkl()
        >> data_pipeline
    )

    save_filepath = ROOT_DIR/'data'/'interim'/'ber_clean.pkl'
    ber.to_pickle(save_filepath)


@pipes
def data_pipeline(ber: pd.DataFrame) -> pd.DataFrame:

    return (
        _pull_out_columns(ber)
        >> _rename_columns
        >> _pull_out_dublin
        >> _remove_extreme_outliers
        >> _replace_null_values
        >> _set_lowercase
        >> _set_index_to_postcodes
        >> _decode_column_suppl_wh_fuel
        >> _decode_column_chp_system_type
        >> _create_cso_period_built_column
        >> _create_cso_dwelling_type_column
        >> _calculate_total_floor_area
        >> _drop_non_essential_columns
    )


def _load_ber_csv() -> pd.DataFrame:

    ber_load_path = DATA_DIR/'raw'/f'{INPUT_NAME}.txt'

    return pd.read_csv(
        ber_load_path,
        sep='\t',
        low_memory=False,
        encoding='latin-1',
        error_bad_lines=False,
    )


def _save_ber_as_pkl() -> None:

    ber = _load_ber_csv()
    ber_save_path = DATA_DIR/'interim'/f'{INPUT_NAME}.pkl'
    ber.to_pickle(ber_save_path)


def _load_ber_pkl() -> pd.DataFrame:

    ber_load_path = DATA_DIR/'interim'/f'{INPUT_NAME}.pkl'

    return pd.read_pickle(ber_load_path)


BER_columns = [
    'CountyName',
    'DwellingTypeDescr',
    'Year_of_Construction',

    # Area
    'GroundFloorArea',
    'FirstFloorArea',
    'SecondFloorArea',
    'ThirdFloorArea',

    # HW Primary
    'DeliveredEnergyMainWater',
    'MainWaterHeatingFuel',
    'WHMainSystemEff',
    'WHEffAdjFactor',

    # HW Secondary
    'DeliveredEnergySupplementaryWater',
    'SupplWHFuel',

    # SH Primary
    'DeliveredEnergyMainSpace',
    'MainSpaceHeatingFuel',
    'HSMainSystemEfficiency',
    'HSEffAdjFactor',

    # SH Secondary
    'DeliveredEnergySecondarySpace',
    'HSSupplSystemEff',
    'SupplSHFuel',

    # Other
    'ElecImmersionInSummer',
    'SolarHeatFraction',
    'HSSupplHeatFraction',

    # Group Heating
    'CHPSystemType',
    'CHPUnitHeatFraction',
    'gsdHSSupplHeatFraction',
]


def _pull_out_columns(df: pd.DataFrame) -> pd.DataFrame:

    return df.loc[:, BER_columns]


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:

    return df.rename(columns={'CountyName': 'Postcodes'})


def _pull_out_dublin(df: pd.DataFrame) -> pd.DataFrame:

    dublin_rows = df['Postcodes'].str.contains('Dublin')
    return df[dublin_rows]


def _remove_extreme_outliers(df: pd.DataFrame,) -> pd.DataFrame:

    main_water_heat = df['DeliveredEnergyMainWater']
    main_space_heat = df['DeliveredEnergyMainSpace']

    cutoff_water_heat = 300000
    cutoff_space_heat = 600000

    mask1 = (main_water_heat > cutoff_water_heat) | (main_water_heat <= 0)
    mask2 = (main_space_heat > cutoff_space_heat) | (main_space_heat <= 0)

    mask = mask1 | mask2

    return df[~mask]


def _replace_null_values(df: pd.DataFrame) -> pd.DataFrame:

    mask_floats = df.dtypes == np.float
    df.loc[:, mask_floats] = df.loc[:, mask_floats].fillna(0)

    mask_strings = df.dtypes == np.object
    df.loc[:, mask_strings] = df.loc[:, mask_strings].fillna('-')

    return df


def _set_lowercase(df: pd.DataFrame) -> pd.DataFrame:

    mask_strings = df.dtypes == np.object
    df.loc[:, mask_strings] = (
        df.loc[:, mask_strings]
        .stack()
        .str.lower()
        .unstack()
    )

    return df


def _set_index_to_postcodes(df: pd.DataFrame) -> pd.DataFrame:

    return df.set_index('Postcodes')


def _decode_column_suppl_wh_fuel(df: pd.DataFrame,) -> pd.DataFrame:
    ''' Only one type of supplementary HW system recognised in the BER,
        namely immersion heater located in the HW cylinder,
        so fuel if specified is always Electricity
        '''

    column = 'SupplWHFuel'

    # ! where condition is true x else y
    df[column] = np.where(
        df[column] == 1,
        'Electricity',
        '',
    )

    return df


def _decode_column_chp_system_type(df: pd.DataFrame,) -> pd.DataFrame:

    column = 'CHPSystemType'

    conditions = [
        (df[column] == 1),
        (df[column] == 2),
    ]

    choices = [
        'CHP',
        'Waste heat from power plant',
    ]

    # ! where condition is true pick corresponding choice
    df[column] = np.select(
        conditions,
        choices,
        default='',
    )

    return df


def _create_cso_period_built_column(df: pd.DataFrame) -> pd.DataFrame:

    column = 'Year_of_Construction'
    new_column = 'Period_Built'
    array = df[column]

    conditions = [
        array < 1920,
        array < 1946,
        array < 1961,
        array < 1971,
        array < 1981,
        array < 1991,
        array < 2001,
        array < 2006,
        array < 2025,
        array == 0,
    ]

    choices = [
        'before 1919',
        '1919 - 1945',
        '1946 - 1960',
        '1961 - 1970',
        '1971 - 1980',
        '1981 - 1990',
        '1991 - 2000',
        '2001 - 2005',
        '2006 or later',
        'not stated',
    ]

    df[new_column] = np.select(conditions, choices, default='ERROR')

    return df


def _create_cso_dwelling_type_column(df: pd.DataFrame) -> pd.DataFrame:

    column = 'DwellingTypeDescr'
    new_column = 'Dwelling_Type'
    array = df[column]

    conditions = [
        (
            (array == 'mid-floor apartment')
            | (array == 'ground-floor apartment')
            | (array == 'top-floor apartment')
            | (array == 'apartment')
            | (array == 'basement dwelling')
            | (array == 'maisonette')
        ),
        (
            (array == 'end of terrace house')
            | (array == 'mid-terrace house')
            | (array == 'house')
        ),
        (
            (array == 'semi-detached house')
        ),
        (
            (array == 'detached house')
        ),
    ]

    choices = [
        'apartment',
        'terraced house',
        'semi-detached house',
        'detached house',
    ]

    df[new_column] = np.select(conditions, choices, default='ERROR')

    return df


def _calculate_total_floor_area(df: pd.DataFrame) -> pd.DataFrame:

    df['TotalFloorArea'] = (
        df['GroundFloorArea']
        + df['FirstFloorArea']
        + df['SecondFloorArea']
        + df['ThirdFloorArea']
    )

    return df


def _calc_true_main_space_heat_demand(df: pd.DataFrame) -> pd.DataFrame:

    chp_system_type = df['CHPSystemType'].to_numpy()
    secondary_heat_supply_fraction = df['gsdHSSupplHeatFraction'].to_numpy()
    heat_energy_demand_main_space = df['DeliveredEnergyMainSpace'].to_numpy()
    effficiency_boier = df['HSMainSystemEfficiency'].to_numpy()
    efficiency_adjustment_factor = df['HSEffAdjFactor'].to_numpy()
    chp_unit_heat_fraction = df['CHPUnitHeatFraction'].to_numpy()

    efficiency_boiler_adjusted = (
        effficiency_boier * efficiency_adjustment_factor
    )

    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    if (
        (chp_system_type == 'CHP')
        or (chp_system_type == 'Waste heat from power stations')
    ):
        ''' EXCEL FORMULA: (1-K6) * ShReqt / H12
            K6 = Fraction of heat use from secondary
                 / supplementary system (from Table 7, Table 10 or Appendix F)
            ShReqt = Annual space heating requirement [kWh/y]
                   = Net heat emission to heated space [kWh/y]
                     + Additional heat loss [kWh/y]
            H12 = Efficiency factor for charging method [-]
                = 0.9
            '''
        demand = (
            (1 / 1 - secondary_heat_supply_fraction)
            * heat_energy_demand_main_space
            * 0.9
        )

    # Individual Heating
    elif chp_unit_heat_fraction < 1:
        ''' EXCEL FORMULA: IF(J7=0,0,(1-M21) * F125 / (J7/100))
            J7 = Adjusted efficiency of main heating system [%]
            M21 = Fraction of main space and water heat from CHP
            F125 = Total heat demand main space heating [kWh/y]
            '''
        demand = (
            (1 / 1 - chp_unit_heat_fraction)
            * heat_energy_demand_main_space
            * (efficiency_boiler_adjusted / 100)
        )

    else:
        demand = 0

    df['Heat Demand - Main Space'] = demand

    return df


def _drop_non_essential_columns(ber: pd.DataFrame) -> pd.DataFrame:

    columns = [
        'Period_Built',
        'Dwelling_Type',
        'DeliveredEnergyMainWater',
        'DeliveredEnergyMainSpace',
    ]

    return ber[columns]

# def get_true_main_SH_demand(df: pd.DataFrame) -> pd.DataFrame:

#     df[r'DeliveredEnergyMainSpace (ind)'] = np.around(
#         calc_true_main_SH_demand(
#             df['CHPSystemType'].values,
#             df['gsdHSSupplHeatFraction'].values,
#             df[DeliveredEnergyMainSpace'].values,
#             df['HSMainSystemEfficiency'].values,
#             df['HSEffAdjFactor'].values,
#             df['CHPUnitHeatFraction'].values,
#         )
#     )

#     return df


# @np.vectorize
# def calc_true_sec_SH_Demand(
#     chp_system_type,
#     gsdHSSupplHeatFraction,
#     HSSupplHeatFraction,
#     delivered_energy_sec,
#     delivered_energy_main,
#     efficiency,
#     CHPUnitHeatFraction,
# ):

#     # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
#     if chp_system_type == 'CHP' or chp_system_type == 'Waste heat from power stations':
#         ''' EXCEL FORMULA: IF(J8=0,0, J8 * SH!H14 / (J9/100))
#             J8 = Fraction of heat from secondary or supplementary system
#             (from Table 7, Table 10 or Appendix F)
#             SH!H14 = Gross heat emission to heated space [kWh/y]
#             J9 = Efficiency of secondary or supplementary system [%]
#             (from Table 4a or Appendix E)

#             delivered_energy_main = total_SH * (1 - frac_used_by_secondary)
#             delivered_energy_sec = frac_used_by_secondary * total_SH
#                                  = frac_used_by_secondary
#                                     * delivered_energy_main
#                                     / (1 - frac_used_by_secondary)
#             '''
#         demand = (
#             (gsdHSSupplHeatFraction / 1 - gsdHSSupplHeatFraction)
#             * delivered_energy_main
#             * (efficiency / 100)
#         )

#     # Individual Heating
#     if efficiency > 0:
#         ''' EXCEL FORMULA: IF(J8=0,0,J8*SH!H14/(J9/100))
#             J8 = Fraction of heat from secondary / supplementary system
#             (from Table 7, Table 10 or Appendix F)
#             SH!H14 = Gross heat emission to heated space [kWh/y]
#             J9 = Efficiency of secondary / supplementary system [%]
#             (from Table 4a or Appendix E)
#             '''
#         demand = HSSupplHeatFraction * \
#             delivered_energy_main / (efficiency / 100)
#     else:
#         demand = 0

#     return demand


# def get_true_sec_SH_demand(df: pd.DataFrame) -> pd.DataFrame:

#     df[r'DeliveredEnergySecondarySpace (ind)'] = np.around(
#         calc_true_sec_SH_Demand(
#             df['CHPSystemType'].values,
#             df['gsdHSSupplHeatFraction'].values,
#             df['HSSupplHeatFraction'].values,
#             df['DeliveredEnergySecondarySpace'].values,
#             df[r'DeliveredEnergyMainSpace (ind)'].values,
#             df['HSSupplSystemEff'].values,
#             df['CHPUnitHeatFraction'].values,
#         )
#     )

#     return df


# @np.vectorize
# def calc_true_main_HW_Demand(
#     CHPSystemType,
#     gsdHSSupplHeatFraction,
#     delivered_energy_main,
#     effficiency,
#     efficiency_adjustment_factor,
#     CHPUnitHeatFraction,
# ):
#     eff_adj = effficiency * efficiency_adjustment_factor

#     # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
#     if CHPSystemType == 'CHP' or CHPSystemType == 'Waste heat from power stations':
#         ''' EXCEL FORMULA: WhReqt/H12
#             WhReqt = Output from main water heater [kWh/y]
#             H12 = Efficiency factor for charging method [-] = 0.9
#             '''
#         demand = delivered_energy_main * 0.9

#     # Individual Heating
#     if CHPUnitHeatFraction < 1:
#         ''' EXCEL FORMULA: IF(J16=0,0,(1-M21)*WhReqt/(J16/100))
#             J16 = Adjusted efficiency of main water heater [%]
#             WhReqt = Output from main water heater [kWh/y]
#             M21 = Fraction of main space and water heat from CHP
#             '''
#         demand = (1 / 1 - CHPUnitHeatFraction) * \
#             delivered_energy_main * (eff_adj / 100)

#     else:
#         demand = 0

#     return demand


# def get_true_main_HW_demand(df: pd.DataFrame) -> pd.DataFrame:

#     df[r'DeliveredEnergyMainWater (ind)'] = np.around(
#         calc_true_main_HW_Demand(
#             df['CHPSystemType'].values,
#             df['gsdHSSupplHeatFraction'].values,
#             df['DeliveredEnergyMainWater'].values,
#             df['WHMainSystemEff'].values,
#             df['WHEffAdjFactor'].values,
#             df['CHPUnitHeatFraction'].values,
#         )
#     )

#     return df


# @np.vectorize
# def calc_true_sec_HW_Demand(delivered_energy_sec,):
#     # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
#     ''' EXCEL FORMULA: IF(WhReqtSup=0,0,NA())
#         NA() = Output from supplementary heater [kWh/y]
#         '''
#     # Individual Heating
#     ''' EXCEL FORMULA: WhReqtSup
#         WhReqtSup = Energy required for supplementary electric water heating [kWh/y]
#                   = Output from supplementary heater [kWh/y]
#         '''

#     return delivered_energy_sec


# def get_true_sec_HW_demand(df: pd.DataFrame) -> pd.DataFrame:

#     df[r'DeliveredEnergySupplementaryWater (ind)'] = np.around(
#         calc_true_sec_HW_Demand(
#             df['DeliveredEnergySupplementaryWater'].values,)
#     )

#     return df


# # --------------------------------------------------------------------------


# def print_demands_made_ind() -> None:

#     print('BER heat demand estimates have been made independent of technology!')

#     return None


# # --------------------------------------------------------------------------


# def load_in_Dublin_BER_data() -> pd.DataFrame:

#     filename = 'BERPublicsearch.pkl'
#     path_to_raw_data = path.get(filename)

#     df_Dublin = pipe(
#         load.local_data(path_to_raw_data),
#         pull_out_Dublin_data,
#         pull_out_columns,
#         rename_CountyName_to_Postcodes,
#     )

#     return df_Dublin


# def execute_data_pipeline(df: pd.DataFrame) -> pd.DataFrame:

#     df_clean = pd.DataFrame()

#     remove_outliers(df)
#     copy_across_useful_columns(df, df_clean)

#     decode_column_SupplWHFuel(df, df_clean)
#     decode_column_CHPSystemType(df, df_clean)
#     clean_columns(df, df_clean)

#     create_CSO_columns(df, df_clean)
#     calculate_total_floor_area(df, df_clean)

#     get_true_main_SH_demand(df_clean)
#     get_true_sec_SH_demand(df_clean)
#     get_true_main_HW_demand(df_clean)
#     print_demands_made_ind()

#     return df_clean


# --------------------------------------------------------------------------

# if __name__ == '__main__':

#     # df = load_in_Dublin_BER_data()
#     # df_clean = execute_data_pipeline(df)
#     print('All __name__ methods have been commented out')
#     pass
