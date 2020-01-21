import sys

# ----------------------------------
# TEST FUNCTIONS ETC WITH PYTEST



# ----------------------------------

def df (df,path):
    """Save data from data name

    Args:
        df (pd.DataFrame): The data in DataFrame format
        data_name (str)

    Returns:
        None

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    import pandas as pd

    if '.pkl' in path:
        df.to_pickle(path)

    elif '.csv' in path:
        df.to_csv(path)
        
    elif '.xlsx':
        df.to_excel(path)

    # ------------------------------------
    # Errors
    # ------------------------------------
    else:
        raise ValueError (r"Can only save .pkl or .csv files")

    # *************
    # debug comment
    # *************
    print(f"Df saved @{path}...")



# -----------------------------------------------------------------
# -----------------------------------------------------------------


if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])
