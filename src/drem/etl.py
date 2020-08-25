#!/usr/bin/env python

import warnings

import prefect

from prefect import Flow
from prefect import Parameter

import drem

from drem.filepaths import DATA_DIR


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


with Flow("Extract, Transform & Load DREM Data") as flow:

    data_dir = Parameter("data_dir", default=DATA_DIR)
    external_dir = data_dir / "external"
    processed_dir = data_dir / "processed"

    sa_geometries_raw = drem.extract_sa_geometries(external_dir)
    sa_statistics_raw = drem.extract_sa_statistics(external_dir)
    sa_statistics_glossary = drem.extract_sa_glossary(external_dir)
    dublin_postcodes_raw = drem.extract_dublin_postcodes(external_dir)

    sa_geometries_clean = drem.transform_sa_geometries(sa_geometries_raw)
    dublin_postcodes_clean = drem.transform_dublin_postcodes(dublin_postcodes_raw)
    sa_statistics = drem.transform_sa_statistics(
        sa_statistics_raw,
        sa_statistics_glossary,
        sa_geometries_clean,
        dublin_postcodes_clean,
    )

    drem.load_sa_geometries(sa_geometries_clean, processed_dir)
    drem.load_sa_statistics(sa_statistics, processed_dir)
