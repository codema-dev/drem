"""

This script will create a synthetic residential building stock to divide
each SA into several building types. These ratios will then be applied to
outputs from EnergyPlus to generate a first-pass estimate for residential
energy demand in Dublin

"""

import pandas as pd
import geopandas as gpd

from drem.filepaths import RAW_DIR

