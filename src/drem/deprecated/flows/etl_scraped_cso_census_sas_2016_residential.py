from prefect import Flow
from pipeop import pipes


from codema_drem._filepaths import (
    DATA_DIR,
    SAS_CLEAN,
    CENSUS_PERIOD_BUILT_RAW,
    CENSUS_DWELLING_TYPE_RAW,
    CENSUS_PERIOD_BUILT_CLEAN,
    CENSUS_DWELLING_TYPE_CLEAN,
)
from codema_drem.utilities.flow import run_flow
from codema_drem.tasks.etl_scraped_census_sas_2016 import (
    _load_csv,
    _extract_columns_period_built_data,
    _rename_columns_period_built_data,
    _rename_period_built_to_same_as_census,
    _extract_columns_dwelling_type_data,
    _drop_rows_period_built_data,
    _link_period_built_and_dwelling_type_data,
)


@pipes
def flow_etl_scraped_census_sas() -> Flow:

    with Flow("Link Scraped Census to EDs") as flow:

        year_built_by_sa = (
            _load_csv(CENSUS_PERIOD_BUILT_RAW)
            >> _extract_columns_period_built_data
            >> _rename_columns_period_built_data
            >> _rename_period_built_to_same_as_census
        )

        dwelling_type_by_sa = (
            _load_csv(CENSUS_DWELLING_TYPE_RAW)
            >> _extract_columns_dwelling_type_data
            >> _rename_columns_dwelling_type_data
            >> _rename_dwelling_type_to_same_as_census
            >> _drop_rows_period_built_data
        )

        _link_period_built_and_dwelling_type_data(year_built_by_sa, dwelling_type_by_sa)

        # sas = _load_geodataframe(SAS_CLEAN)

        # year_built_by_sa_with_eds = _link_census_to_eds(year_built_by_sa, sas)
        # _save_to_parquet(year_built_by_sa_with_eds, CENSUS_PERIOD_BUILT_CLEAN)

        # dwelling_type_by_sa_with_eds = _link_census_to_eds(dwelling_type_by_sa, sas)
        # _save_to_parquet(dwelling_type_by_sa_with_eds, CENSUS_DWELLING_TYPE_CLEAN)

    return flow
