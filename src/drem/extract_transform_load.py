#!/usr/bin/env python

import prefect

from prefect import Flow

import drem


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

        cso_sa_geometries_filepath = drem.extract_cso_sa_geometries()
        cso_sa_geometries = drem.transform_cso_sa_geometries(cso_sa_geometries_filepath)
        drem.load_cso_sa_geometries(cso_sa_geometries)

    return flow
