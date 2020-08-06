#!/usr/bin/env python

import prefect
from prefect import Flow

from drem.extract.cso_sa_geometries import extract_cso_sa_geometries
from drem.load.cso_sa_geometries import load_cso_sa_geometries
from drem.transform.cso_sa_geometries import transform_cso_sa_geometries

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


def etl() -> Flow:
    """Extract, Transform & Load drem data for modelling.

    A Prefect flow to orchestrate the Extraction, Transformation and Loading
    of drem data.

    For more information see: https://docs.prefect.io/core/tutorial/01-etl-before-prefect.html

    Returns
    -------
    Flow
        A flow pipeline of Prefect Tasks to be executed
    """
    with Flow("Extract, Transform & Load DREM Data") as flow:

        cso_sa_geometries_filepath = extract_cso_sa_geometries()
        cso_sa_geometries = transform_cso_sa_geometries(cso_sa_geometries_filepath)
        load_cso_sa_geometries(cso_sa_geometries)

    return flow
