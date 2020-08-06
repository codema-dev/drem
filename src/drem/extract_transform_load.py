from os import environ

import prefect
from prefect import Flow, task
from prefect.engine.results import LocalResult
from prefect_toolkit import run_flow

from drem import extract
from drem import transform
from drem import load
from drem._filepaths import INTERIM_DIR, MNR_RAW, MNR_CLEAN, VO_RAW, VO_CLEAN

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


def etl() -> Flow:

    with Flow(
        "Extract, Transform & Load DREM Data",
        # result=LocalResult(str(INTERIM_DIR))
    ) as flow:

        cso_sa_geometries_filepath = extract.cso_sa_geometries.run()
        cso_sa_geometries = transform.cso_sa_geometries.run(cso_sa_geometries_filepath)
        load.cso_sa_geometries.run(cso_sa_geometries)

    return flow

