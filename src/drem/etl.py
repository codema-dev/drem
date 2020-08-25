#!/usr/bin/env python

import warnings

import prefect

from prefect import Flow

import drem

from drem.filepaths import EXTERNAL_DIR


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


with Flow("Extract, Transform & Load DREM Data") as flow:

    sa_geometries_raw = drem.extract_sa_geometries(EXTERNAL_DIR)
    sa_statistics_raw = drem.extract_sa_statistics(EXTERNAL_DIR)
    sa_statistics_glossary = drem.extract_sa_glossary(EXTERNAL_DIR)
    dublin_postcodes_raw = drem.extract_dublin_postcodes(EXTERNAL_DIR)

    sa_geometries_clean = drem.transform_sa_geometries(sa_geometries_raw)
    dublin_postcodes_clean = drem.transform_dublin_postcodes(dublin_postcodes_raw)
    sa_statistics = drem.transform_sa_statistics(
        sa_statistics_raw,
        sa_statistics_glossary,
        sa_geometries_clean,
        dublin_postcodes_clean,
    )

    drem.load_sa_geometries(sa_geometries_clean)
    drem.load_sa_statistics(sa_statistics)
