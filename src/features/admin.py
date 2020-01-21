from os import rename, listdir, getcwd
from sys import argv
from pprint import pprint

def add_or_subtract_to_all_file_numbers(A, change, *args):
    """ Docstring:
    
        Adds 'change' to all file numbers >= 'A'
        
        Parameters
        ----------
        
        A : int
            The file number from which to add 'change' to
        change : int
            The number to be added to all affected file name numbers
   
        Returns
        -------
        
        Changes the file name numbers of all affected files in directory
        
        """

    cwd = getcwd()
    file_names = listdir()
    
    A = int(A)
    change = int(change)

    # Find all file names to be changed
    file_name_changes = {}
    for file_name in file_names:

        if file_name[0].isdigit():

            file_number = int(file_name[0])
            if file_number >= A:
                
                new_name = str(file_number + change) + file_name[1:]
                file_name_changes[file_name] = new_name

    # Get paths to all names to be changed and replacement names
    orig_file_names = list(file_name_changes.keys())
    new_file_names = list(file_name_changes.values())

    # Rename all files
    for old_name, new_name in zip(orig_file_names, new_file_names):

        rename(cwd + "\\" + old_name, cwd + "\\" + new_name)
        

# -----------------------------------------------------------------
# -----------------------------------------------------------------


def remove_whitespace_from_names ():
    
    cwd = getcwd()
    file_names = listdir()

    # Find all file names to be changed
    file_name_changes = {}
    for file_name in file_names:

        new_name = file_name.replace(".","",1).replace(" ","_")
        file_name_changes[file_name] = new_name
            
    # Get paths to all names to be changed and replacement names
    orig_file_names = list(file_name_changes.keys())
    new_file_names = list(file_name_changes.values())

    # Rename all files
    for old_name, new_name in zip(orig_file_names, new_file_names):
        
        rename(cwd + "\\" + old_name, cwd + "\\" + new_name)

    
# -----------------------------------------------------------------
# -----------------------------------------------------------------



""" Commented Out Code """

# def change_file_letters_to_numbers ():
#     """ Docstring:
   
#         Parameters
#         ----------
        
#         """

#     cwd = getcwd()
#     file_names = listdir()

#     # Find all file names to be changed
#     file_name_changes = {}
#     for counter,file_name in enumerate(file_names,1):
        
#         if file_name[0].isalpha():
#             new_name = str(counter) + file_name[1:]
#             file_name_changes[file_name] = new_name

#     # Get paths to all names to be changed and replacement names
#     orig_file_names = list(file_name_changes.keys())
#     new_file_names = list(file_name_changes.values())

#     # Rename all files
#     for old_name, new_name in zip(orig_file_names, new_file_names):

#         rename(cwd + "\\" + old_name, cwd + "\\" + new_name)


# -----------------------------------------------------------------
# -----------------------------------------------------------------
    