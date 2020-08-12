#!/usr/bin/env python

import prefect

from prefect import Flow

import drem

from drem.filepaths import EXTERNAL_DIR


# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


with Flow("Extract, Transform & Load DREM Data") as flow:

    sa_geometries_raw = drem.extract_sa_geometries(EXTERNAL_DIR)
    sa_geometries_clean = drem.transform_sa_geometries(sa_geometries_raw)
    drem.load_sa_geometries(sa_geometries_clean)

    sa_statistics_raw = drem.extract_sa_statistics(EXTERNAL_DIR)
    sa_statistics_glossary = drem.extract_sa_glossary(EXTERNAL_DIR)
    sa_statistics = drem.transform_sa_statistics(
        sa_statistics_raw, sa_statistics_glossary, sa_geometries_clean,
    )
    drem.load_sa_statistics(sa_statistics)
