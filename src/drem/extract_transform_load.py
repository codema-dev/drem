#!/usr/bin/env python

import prefect

from prefect import Flow

import drem


# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


with Flow("Extract, Transform & Load DREM Data") as flow:

    cso_sa_geometries_raw = drem.extract_cso_sa_geometries()
    cso_sa_geometries_clean = drem.transform_cso_sa_geometries()
    drem.load_cso_sa_geometries(cso_sa_geometries_clean)

    cso_sa_statistics_raw = drem.extract_cso_sa_statistics()
    cso_sa_statistics_glossary = drem.extract_cso_sa_glossary()
    cso_sa_statistics = drem.transform_cso_sa_statistics(
        cso_sa_statistics_raw, cso_sa_statistics_glossary,
    )
    drem.load_cso_sa_statistics(cso_sa_statistics)
