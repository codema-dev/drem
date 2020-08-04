from prefect import Flow
from pipeop import pipes

from codema_drem.tasks.etl_requested_census_eds_2016 import (
    _load_parquet,
    _group_period_built_data_by_ed,
    _group_dwelling_type_data_by_ed,
    _load_census2016_raw,
    _reformat_data,
    _replace_all_less_than_six_values_with_zero,
    _replace_all_households_with_scraped_all_households,
    _replace_all_other_less_than_values_with_themselves,
    _set_column_data_types,
    _amalgamate_d_semid_terraced_into_house,
    _replace_all_years_with_scraped_all_years,
    _add_column_for_number_of_persons_in_each_period_built_hh_from_scraped,
    _infer_values_for_less_than_six_groupings,
    _infer_values_for_not_stated_groupings,
    _drop_totals_and_not_stated,
    _replace_negative_value_groupings_with_zero,
    _rename_electoral_districts_so_compatible_with_cso_map,
    _reset_column_data_types,
    _save_dataframe_result,
)
from codema_drem._filepaths import (
    DATA_DIR,
    SAS_CLEAN,
    CENSUS_PERIOD_BUILT_RAW,
    CENSUS_DWELLING_TYPE_RAW,
    CENSUS_PERIOD_BUILT_CLEAN,
    CENSUS_DWELLING_TYPE_CLEAN,
)
from codema_drem.utilities.flow import run_flow


@pipes
def census2016_etl_flow() -> Flow:

    with Flow("Preprocess Census2016") as flow:

        period_built_scraped_ed_totals = (
            _load_parquet(CENSUS_PERIOD_BUILT_CLEAN) >> _group_period_built_data_by_ed
        )

        dwelling_type_scraped_ed_totals = (
            _load_parquet(CENSUS_DWELLING_TYPE_CLEAN) >> _group_dwelling_type_data_by_ed
        )

        census_clean = (
            _load_census2016_raw(CENSUS_RAW)
            >> _reformat_data
            >> _replace_all_less_than_six_values_with_zero
            >> _replace_all_households_with_scraped_all_households(
                period_built_scraped_ed_totals
            )
            >> _replace_all_other_less_than_values_with_themselves
            >> _set_column_data_types
            >> _amalgamate_d_semid_terraced_into_house
            >> _replace_all_years_with_scraped_all_years(
                dwelling_type_scraped_ed_totals
            )
            >> _add_column_for_number_of_persons_in_each_period_built_hh_from_scraped(
                period_built_scraped_ed_totals
            )
            >> _infer_values_for_less_than_six_groupings
            >> _infer_values_for_not_stated_groupings
            >> _drop_totals_and_not_stated
            >> _replace_negative_value_groupings_with_zero
            >> _rename_electoral_districts_so_compatible_with_cso_map
            >> _reset_column_data_types
        )

        _save_dataframe_result(census_clean)

    return flow


# NOTE:
# Total housing stock is 530,753 from http://census.cso.ie/sapmap/
# census.total_hh.sum() = 443,825...

# NOTE: run_flow(flow_function=census2016_etl_flow, viz_path=CENSUS_FLOW)
