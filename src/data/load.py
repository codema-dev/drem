import sys

# -----------------------------------------------------------------
# -----------------------------------------------------------------


def local_data(filepath):
    """Load data from path string

    Args:
        path (str)

    Returns:
        pd.DataFrame: Data in DataFrame format

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    import pandas as pd
    import geopandas as gpd

    if not filepath:
        raise ValueError("Error: No path argument entered!")

    # *************
    # debug comment
    # *************
    print("Attempting to load data...")

    if "BER" and ".txt" in str(filepath):
        data = pd.read_csv(
            filepath,
            sep="\t",
            low_memory=False,
            encoding="latin-1",
            error_bad_lines=False,
        )

    elif ".xlsx" in str(filepath):
        data = pd.read_excel(filepath, engine="openpyxl")

    elif ".csv" in str(filepath) or ".txt" in str(filepath):
        data = pd.read_csv(filepath)

    elif ".pkl" in str(filepath):
        data = pd.read_pickle(filepath)

    elif ".shp" in str(filepath):
        data = gpd.read_file(filepath)

    else:
        raise ValueError("File type doesn't match!")

    # *************
    # debug comments
    # *************
    print("Data successfully loaded!")

    return data


# -----------------------------------------------------------------
# -----------------------------------------------------------------

if __name__ == "__main__":
    globals()[sys.argv[1]](sys.argv[2])
