from geopy import Nominatim, GoogleV3
import pytest
import pandas as pd


""" Useful boilerplate prefect setup:

    state = pipeline_result_state
    flow = pipeline_flow

    vo_before_ref = flow.get_tasks(name='<FUNC NAME>')[0]
    vo_after_ref = flow.get_tasks(name='<FUNC NAME>')[0]

    vo_before = state.result[vo_before_ref]._result.value
    vo_after = state.result[vo_after_ref]._result.value
    """


# @pytest.fixture()
# def pipeline_result_state() -> prefect.engine.state.Success:

#     flow = vo_etl_flow()
#     return flow.run()


# @pytest.fixture()
# def pipeline_flow() -> prefect.core.flow.Flow:

#     return vo_etl_flow()


# def test_geolocation_returns_correct_type(
#     pipeline_result_state: prefect.engine.state.Success,
#     pipeline_flow: prefect.core.flow.Flow,
# ):

#     state = pipeline_result_state
#     flow = pipeline_flow

#     vo_after_ref = flow.get_tasks(name='Geocode alternative addresses')[0]
#     vo_after = state.result[vo_after_ref]._result.value

#     assert vo_after['geocoded_address']
