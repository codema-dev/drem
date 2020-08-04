from shutil import rmtree
from typing import List, Tuple

import numpy as np
import pandas as pd
from icontract import ensure, require
from pipeop import pipes
from prefect import Flow, task


from codema_drem._filepaths import (
    INTERIM_DIR,
    BER_CLEAN,
    BER_FLOW,
    BER_RAW,
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
)
from codema_drem.utilities.flow import run_flow
from codema_drem.utilities.preprocessing import (  # infer_column_data_types,
    convert_mostly_empty_float_columns_to_sparse,
    convert_string_columns_to_categorical,
    downcast_float_columns,
    downcast_int_columns,
    replace_blanks_with_zero_in_float_columns,
    set_string_columns_to_lowercase,
)

# NOTE - fastparquet or pyarrow are required for parquet storage...

np.seterr(all="raise")  # force numpy runtime errors to stop code


# INTERMEDIATE RESULTS
BER_RAW_PARQUET = DATA_DIR / "interim" / "BERPublicsearch.parquet"


@pipes
def ber_etl_flow() -> Flow:

    with Flow("Preprocess BER data") as flow:
        ber_clean = (
            _load_ber()
            >> _pull_out_dublin_rows
            >> set_string_columns_to_lowercase
            >> convert_string_columns_to_categorical
            >> downcast_int_columns
            >> downcast_float_columns
            >> replace_blanks_with_zero_in_float_columns
            # >> convert_mostly_empty_float_columns_to_sparse
            >> _rename_columns
            >> _drop_outliers
            >> _drop_imaginary_postcodes
            >> _create_cso_dwelling_type_column
            >> _drop_unmatched_dwelling_types
            >> _create_cso_period_built_column
            >> _decode_column_suppl_wh_fuel
            >> _decode_column_chp_system_type
            >> _calculate_total_floor_area
        )

        _save_dataframe_result(ber_clean)

    return flow


# NOTE: run_flow(flow_function=ber_etl_flow, viz_path=BER_FLOW)


def delete_intermediate_results() -> None:
    """ Delete files created during flow if wish to recalculate intermediate
        results
        """

    rmtree(BER_TEMP)


# ---------------------------------------------------------------------------


@require(lambda BER_RAW: "CountyName" in pd.read_table(BER_RAW, nrows=1))
def _load_raw_ber() -> pd.DataFrame:

    return pd.read_table(
        BER_RAW, sep="\t", low_memory=False, encoding="latin-1", error_bad_lines=False,
    )[lambda x: x["CountyName"].str.contains("Dublin")]


@task
def _load_ber() -> pd.DataFrame:

    # Save BERPublicsearch as a pickle file for faster load times on reruns
    if BER_RAW_PARQUET.exists():
        ber = pd.read_parquet(BER_RAW_PARQUET)
    else:
        ber = _load_raw_ber()
        ber.to_parquet(BER_RAW_PARQUET)

    return ber


@task
@require(lambda ber: "CountyName" in ber.columns)
@ensure(lambda result: not result.empty)
def _pull_out_dublin_rows(ber: pd.DataFrame) -> pd.DataFrame:

    dublin_rows = ber["CountyName"].str.contains("Dublin")
    return ber[dublin_rows]


@task
@require(lambda ber: "CountyName" in ber.columns)
def _rename_columns(ber: pd.DataFrame) -> pd.DataFrame:

    return ber.rename(columns={"CountyName": "postcodes"})


@task
@require(lambda ber: "postcodes" in ber.columns)
@require(lambda ber: ber.postcodes.str.islower().any())
@ensure(
    lambda result: not result.postcodes.isin(
        ["dublin 19", "dublin 21", "dublin 23"]
    ).any()
)
def _drop_imaginary_postcodes(ber: pd.DataFrame) -> pd.DataFrame:
    """After much confusion it was found that none of Dublin 19, 21 or 23
        actually exist.  This function removes them

        Parameters
        ----------
        ber : pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """

    imaginary_postcodes = ["dublin 19", "dublin 21", "dublin 23"]
    return ber[~ber.postcodes.isin(imaginary_postcodes)]


@task
def _drop_outliers(ber: pd.DataFrame,) -> pd.DataFrame:

    main_water_heat = ber["DeliveredEnergyMainWater"]
    main_space_heat = ber["DeliveredEnergyMainSpace"]

    cutoff_water_heat = 300000
    cutoff_space_heat = 600000

    mask1 = (main_water_heat > cutoff_water_heat) | (main_water_heat <= 0)
    mask2 = (main_space_heat > cutoff_space_heat) | (main_space_heat <= 0)

    return ber[~(mask1 | mask2)]


@task
def _create_cso_dwelling_type_column(ber: pd.DataFrame) -> pd.DataFrame:
    """Replace BER dwelling type categories with Census categories

    Note! 
    basement dwelling | house | maisonnette categories are ignored as these
    types are too vague to fit into a Census category

    Parameters
    ----------
    ber : pd.DataFrame

    Returns
    -------
    pd.DataFrame
        ber with a column containing census categories
    """
    with_census_types = {
        "mid-floor apartment": "apartments",
        "ground-floor apartment": "apartments",
        "top-floor apartment": "apartments",
        "apartment": "apartments",
        "end of terrace house": "terraced house",
        "mid-terrace house": "terraced house",
        "semi-detached house": "semi-detached house",
        "detached house": "detached house",
    }

    return ber.assign(dwelling_type=ber.DwellingTypeDescr.map(with_census_types))


@task
@require(lambda ber: "dwelling_type" in ber.columns)
def _drop_unmatched_dwelling_types(ber: pd.DataFrame) -> pd.DataFrame:
    """Drops all HH that didn't meet the criteria defined in 
    _create_cso_dwelling_type_column, includies following HH types that are 
    too vague to assign to a category: maisonnette, basement dwelling, house 

    Parameters
    ----------
    ber : pd.DataFrame
        [description]

    Returns
    -------
    pd.DataFrame
        [description]
    """
    return ber[ber.dwelling_type.notnull()]


@task
def _create_cso_period_built_column(ber: pd.DataFrame) -> pd.DataFrame:

    old_column = "Year_of_Construction"
    new_column = "period_built"
    array = ber[old_column].to_numpy()

    conditions = [
        array <= 1919,
        array < 1946,
        array < 1961,
        array < 1971,
        array < 1981,
        array < 1991,
        array < 2001,
        array < 2010,
        array < 2025,
        array == 0,
    ]

    choices = [
        "before 1919",
        "1919 - 1945",
        "1946 - 1960",
        "1961 - 1970",
        "1971 - 1980",
        "1981 - 1990",
        "1991 - 2000",
        "2001 - 2010",
        "2011 or later",
        "not stated",
    ]

    ber[new_column] = np.select(conditions, choices, default="ERROR")

    return ber


@task
def _decode_column_suppl_wh_fuel(ber: pd.DataFrame,) -> pd.DataFrame:
    """ Only one type of supplementary HW system recognised in the BER,
        namely immersion heater located in the HW cylinder,
        so fuel if specified is always Electricity
        """

    return ber.assign(suppl_hw_fuel=ber["SupplWHFuel"].map({1: "electricity", 0: "",}))


@task
def _decode_column_chp_system_type(ber: pd.DataFrame) -> pd.DataFrame:

    return ber.assign(
        chp_system_type=ber["CHPSystemType"].map(
            {1: "CHP", 2: "Waste heat from power plant",}
        )
    )


@task
def _calculate_total_floor_area(ber: pd.DataFrame) -> pd.DataFrame:

    ber["TotalFloorArea"] = (
        ber["GroundFloorArea"]
        + ber["FirstFloorArea"]
        + ber["SecondFloorArea"]
        + ber["ThirdFloorArea"]
    )

    return ber


@task
def _get_technology_independent_main_space_heat_demand(
    ber: pd.DataFrame,
) -> pd.DataFrame:

    return ber.assign(
        energy_demand_main_space=(
            _calc_technology_independent_main_space_heat_demand(
                chp_system_type=ber["CHPSystemType"].to_numpy(),
                sec_heat_supply_frac=ber["gsdHSSupplHeatFraction"].to_numpy(),
                energy_delivered_main_space=ber["DeliveredEnergyMainSpace"].to_numpy(),
                effficiency_boier=ber["HSMainSystemEfficiency"].to_numpy(),
                efficiency_adjustment_factor=ber["HSEffAdjFactor"].to_numpy(),
                chp_unit_heat_frac=ber["CHPUnitHeatFraction"].to_numpy(),
            )
        )
    )


def _calc_technology_independent_main_space_heat_demand(
    chp_system_type,
    sec_heat_supply_frac,
    delivered_energy_main_space,
    effficiency_boier,
    efficiency_adjustment_factor,
    chp_unit_heat_frac,
) -> pd.DataFrame:

    efficiency_boiler_adjusted = effficiency_boier * efficiency_adjustment_factor

    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    if (chp_system_type == "CHP") or (
        chp_system_type == "Waste heat from power stations"
    ):
        """ EXCEL FORMULA: (1-K6) * ShReqt / H12
            K6 = Fraction of heat use from secondary
                 / supplementary system (from Table 7, Table 10 or Appendix F)
            ShReqt = Annual space heating requirement [kWh/y]
                   = Net heat emission to heated space [kWh/y]
                     + Additional heat loss [kWh/y]
            H12 = Efficiency factor for charging method [-]
                = 0.9
            """
        demand = (1 / 1 - sec_heat_supply_frac) * delivered_energy_main_space * 0.9

    # Individual Heating
    elif chp_unit_heat_frac < 1:
        """ EXCEL FORMULA: IF(J7=0,0,(1-M21) * F125 / (J7/100))
            J7 = Adjusted efficiency of main heating system [%]
            M21 = Fraction of main space and water heat from CHP
            F125 = Total heat demand main space heating [kWh/y]
            """
        demand = (
            (1 / 1 - chp_unit_heat_frac)
            * delivered_energy_main_space
            * (efficiency_boiler_adjusted / 100)
        )

    else:
        demand = 0

    return ber


@task
def _get_technology_independent_sec_space_heat_demand(
    ber: pd.DataFrame,
) -> pd.DataFrame:

    return ber.assign(
        energy_demand_sec_space=_calc_technology_independent_main_space_heat_demand(
            chp_system_type=ber["CHPSystemType"].to_numpy(),
            secondary_heat_supply_frac=ber["gsdHSSupplHeatFraction"].to_numpy(),
            delivered_energy_main_space=ber["DeliveredEnergyMainSpace"].to_numpy(),
            boiler_effficiency=ber["HSMainSystemEfficiency"].to_numpy(),
            efficiency_adjustment_factor=ber["HSEffAdjFactor"].to_numpy(),
            chp_unit_heat_frac=ber["CHPUnitHeatFraction"].to_numpy(),
        )
    )

    return ber


def _calc_technology_independent_main_space_heat_demand(
    chp_system_type,
    secondary_heat_supply_frac,
    delivered_energy_main_space,
    boiler_effficiency,
    efficiency_adjustment_factor,
    chp_unit_heat_frac,
):

    eff_adj = boiler_effficiency * efficiency_adjustment_factor

    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    if (chp_system_type == "CHP") or (
        chp_system_type == "Waste heat from power stations"
    ):
        """ EXCEL FORMULA: (1-K6) * ShReqt / H12
            K6 = Fraction of heat use from secondary
                 / supplementary system (from Table 7, Table 10 or Appendix F)
            ShReqt = Annual space heating requirement [kWh/y]
                   = Net heat emission to heated space [kWh/y]
                     + Additional heat loss [kWh/y]
            H12 = Efficiency factor for charging method [-]
                = 0.9
            """
        demand = (
            (1 / 1 - secondary_heat_supply_frac) * delivered_energy_main_space * 0.9
        )

    # Individual Heating
    elif chp_unit_heat_frac < 1:
        """ EXCEL FORMULA: IF(J7=0,0,(1-M21) * F125 / (J7/100))
            J7 = Adjusted efficiency of main heating system [%]
            M21 = Fraction of main space and water heat from CHP
            F125 = Total heat demand main space heating [kWh/y]
            """
        demand = (
            (1 / 1 - chp_unit_heat_frac) * delivered_energy_main_space * (eff_adj / 100)
        )
    else:
        demand = 0

    return demand


@np.vectorize
def calc_true_sec_SH_Demand(
    chp_system_type,
    gsdHSSupplHeatFraction,
    HSSupplHeatFraction,
    delivered_energy_sec,
    delivered_energy_main,
    efficiency,
    CHPUnitHeatFraction,
):

    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    if chp_system_type == "CHP" or chp_system_type == "Waste heat from power stations":
        """ EXCEL FORMULA: IF(J8=0,0, J8 * SH!H14 / (J9/100))
            J8 = Fraction of heat from secondary or supplementary system
            (from Table 7, Table 10 or Appendix F)
            SH!H14 = Gross heat emission to heated space [kWh/y]
            J9 = Efficiency of secondary or supplementary system [%]
            (from Table 4a or Appendix E)

            delivered_energy_main = total_SH * (1 - frac_used_by_secondary)
            delivered_energy_sec = frac_used_by_secondary * total_SH
                                 = frac_used_by_secondary
                                    * delivered_energy_main
                                    / (1 - frac_used_by_secondary)
            """
        demand = (
            (gsdHSSupplHeatFraction / 1 - gsdHSSupplHeatFraction)
            * delivered_energy_main
            * (efficiency / 100)
        )

    # Individual Heating
    if efficiency > 0:
        """ EXCEL FORMULA: IF(J8=0,0,J8*SH!H14/(J9/100))
            J8 = Fraction of heat from secondary / supplementary system
            (from Table 7, Table 10 or Appendix F)
            SH!H14 = Gross heat emission to heated space [kWh/y]
            J9 = Efficiency of secondary / supplementary system [%]
            (from Table 4a or Appendix E)
            """
        demand = HSSupplHeatFraction * delivered_energy_main / (efficiency / 100)

    else:
        demand = 0

    return demand


@task
def get_true_sec_SH_demand(ber: pd.DataFrame) -> pd.DataFrame:

    ber[r"DeliveredEnergySecondarySpace (ind)"] = np.around(
        calc_true_sec_SH_Demand(
            ber["CHPSystemType"].values,
            ber["gsdHSSupplHeatFraction"].values,
            ber["HSSupplHeatFraction"].values,
            ber["DeliveredEnergySecondarySpace"].values,
            ber[r"DeliveredEnergyMainSpace (ind)"].values,
            ber["HSSupplSystemEff"].values,
            ber["CHPUnitHeatFraction"].values,
        )
    )

    return ber


@np.vectorize
def calc_true_main_HW_Demand(
    CHPSystemType,
    gsdHSSupplHeatFraction,
    delivered_energy_main,
    effficiency,
    efficiency_adjustment_factor,
    CHPUnitHeatFraction,
):
    eff_adj = effficiency * efficiency_adjustment_factor

    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    if CHPSystemType == "CHP" or CHPSystemType == "Waste heat from power stations":
        """ EXCEL FORMULA: WhReqt/H12
            WhReqt = Output from main water heater [kWh/y]
            H12 = Efficiency factor for charging method [-] = 0.9
            """
        demand = delivered_energy_main * 0.9

    # Individual Heating
    if CHPUnitHeatFraction < 1:
        """ EXCEL FORMULA: IF(J16=0,0,(1-M21)*WhReqt/(J16/100))
            J16 = Adjusted efficiency of main water heater [%]
            WhReqt = Output from main water heater [kWh/y]
            M21 = Fraction of main space and water heat from CHP
            """
        demand = (1 / 1 - CHPUnitHeatFraction) * delivered_energy_main * (eff_adj / 100)

    else:
        demand = 0

    return demand


@task
def get_true_main_HW_demand(ber: pd.DataFrame) -> pd.DataFrame:

    ber[r"DeliveredEnergyMainWater (ind)"] = np.around(
        calc_true_main_HW_Demand(
            ber["CHPSystemType"].values,
            ber["gsdHSSupplHeatFraction"].values,
            ber["DeliveredEnergyMainWater"].values,
            ber["WHMainSystemEff"].values,
            ber["WHEffAdjFactor"].values,
            ber["CHPUnitHeatFraction"].values,
        )
    )

    return ber


@np.vectorize
def calc_true_sec_HW_Demand(delivered_energy_sec,):
    # Group Heating (i.e. HH uses CHP or waste energy for HW & SH)
    """ EXCEL FORMULA: IF(WhReqtSup=0,0,NA())
        NA() = Output from supplementary heater [kWh/y]
        """
    # Individual Heating
    """ EXCEL FORMULA: WhReqtSup
        WhReqtSup = Energy required for supplementary electric water heating [kWh/y]
                  = Output from supplementary heater [kWh/y]
        """

    return delivered_energy_sec


@task
def get_true_sec_HW_demand(df: pd.DataFrame) -> pd.DataFrame:

    df[r"DeliveredEnergySupplementaryWater (ind)"] = np.around(
        calc_true_sec_HW_Demand(df["DeliveredEnergySupplementaryWater"].values)
    )

    return df


@task
def _save_dataframe_result(ber: pd.DataFrame) -> None:

    ber.to_parquet(BER_CLEAN)
