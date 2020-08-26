from pathlib import Path

import pyarrow.parquet as pq


def add_file_engine_metadata_to_parquet_file(filepath: Path, file_engine: str) -> None:
    """Add file engine metadata ('pandas' or 'geopandas') to existing parquet file.

    Args:
        filepath (Path): Path to parquet file
        file_engine (str): Engine to be used to read the file
        (options: 'pandas', 'geopandas')
    """
    table = pq.read_table(filepath)

    custom_metadata = {"file_engine": file_engine}
    existing_metadata = table.schema.metadata
    merged_metadata = {**custom_metadata, **existing_metadata}
    fixed_table = table.replace_schema_metadata(merged_metadata)

    pq.write_table(fixed_table, filepath)
