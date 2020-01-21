import pandas as pd
import numpy as np

# Pull numbers from postcodes to use to sort the DataFrame by Postcode number
def pull_numbers_from_postcodes(x):
    if x == "Co. Dublin":
        return -1
    elif x == "Dublin 6W":
        x = x.split()[1]  # Get 6W only where [0,1]==['Dublin','6W']
        return int(x[0])  # Return 6 only
    else:
        x = x.split()
        return int(x[1])
    
pull_numbers_from_postcodes = np.vectorize(pull_numbers_from_postcodes)

def by_postcode(df):
    
    # Sort df by postcode number
    df.reset_index(inplace=True)
    
    df["Postcode Numbers"] = pull_numbers_from_postcodes(df["Postcodes"].values)
    df.sort_values("Postcode Numbers", inplace=True)
    
    df.set_index("Postcodes", inplace=True)
    
    del df["Postcode Numbers"]  # Delete number column
    
    return df