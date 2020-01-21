import sys
import os
from pathlib import Path
from dpcontracts import require, ensure

# Get path to root directory so can access data folder
_ROOT = Path(__file__).parents[2]

def print_ROOT():
    print(_ROOT)

# def get_filenames (directory):
#
#     filenames = _ROOT / f"/data/{directory}"
#
#     # Ignore all hidden files (i.e. start with '.')
#     filenames_clean = [
#         item for item in str(filenames) if item[0] != '.'
#     ]
#
#     return filenames_clean

def get_file_path (filename, filenames, directory):

    # If not a folder or SM data
    if '.' in filename:
        filepath = _ROOT / "data" / f"{directory}" / f"{filename}"

    elif 'SM' in filename:
        filepath = _ROOT / "data" f"{directory}" / f"{filename}"

    elif 'map' in filename:
        filepath = (
            _ROOT / "data" f"{directory}" / f"{filename}" / f"{filename}.shp")

    else:
        raise ValueError(f"{filename} exists in {directory} but not matched!")

    return filepath

@require("`filename` must be a nonempty string",
          lambda args: isinstance(args.filename, str)
                        and len(args.filename) > 0)
def get (filename):
    """Get path from filename

    Args:
        filename (str)

    Returns:
        str: A path to the data

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    import pandas as pd

    directories = ["raw", "interim", "processed", "external"]

    file_exists = False
    for directory in directories:

        directory_path = _ROOT / "data" / f"{directory}"
        filenames = os.listdir(directory_path)
        # get_filenames(directory)

        if filename in filenames:
            file_exists = True
            filepath = get_file_path (filename, filenames, directory)

    if file_exists is False:
        raise ValueError(f"File name \"{filename}\" not found!")

    # *************
    # debug comment
    # *************
    print(f"Accessing data @ {filepath}")

    return filepath

@require("`filename` is required /& must be a string",
    lambda args: (isinstance(args.filename, str) and len(args.filename) > 0))
@require("`directory` is required /& must be a string",
    lambda args: (isinstance(args.directory, str) and len(args.directory) > 0))
@require("""The only valid `directory` options are raw, interim,
            processed /& external""",
    lambda args: args.directory in ["raw", "interim", "processed", "external"])
@require("The only valid filetypes are .pkl, .csv & xlsx",
    lambda args: [type in args.filename for type in ['.pkl','.csv', '.xlsx']])
def set (filename, directory):
    """Set path from data name & directory

    Args:
        data_name (str)
        directory (str): options = raw, interim, processed or external

    Returns:
        str: A path to the data

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    import pandas as pd

    filepath = _ROOT / "data" / f"{directory}" / f"{filename}"

    # *************
    # debug comment
    # *************
    print(f"Setting data @ {filepath}")

    return filepath
