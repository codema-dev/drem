# flake8: noqa

from pathlib import Path

from drem import etl


DATA = Path(__name__) / "data"

etl.flow.run(data_dir=DATA)
