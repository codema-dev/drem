## Create a function to convert integer Postcodes etc. into correct format

from re import findall
import numpy as np

def match_to_full_Postcode_name (postcode):
    """ Docstring:
    
        If postcode != "Co. Dublin" Pull integer out of string and append it to "Dublin "
        
        """
    
    postcode = str(postcode)
    
    if (postcode == "Co.Dublin") or (postcode == "Co.Dublin.1"):
        return "Co. Dublin"
    
    if postcode == "6W":
        return "Dublin 6W"
    
    postcode_number = findall(pattern=r"\d+",string=postcode)
    
    return "Dublin " + postcode_number[0]

match_to_full_Postcode_name = np.vectorize(match_to_full_Postcode_name)