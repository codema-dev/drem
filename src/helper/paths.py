import src
from pathlib import Path

ROOT_DIR = Path(src.__path__[0]).parent
DATA_DIR = ROOT_DIR/'data'
PLOT_DIR = ROOT_DIR/'plots'
LOG_DIR = ROOT_DIR/'logs'
