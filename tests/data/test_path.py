import os
import pytest
from unittest import mock

from src.data import path

def test_file_exists ():

    unknown_file = 'blah.txt'
    known_file = 'testing_file.txt'
    filenames = ['testing_file.txt']
    directory = 'raw'

    assert path.file_exists(known_file, filenames)

    with pytest.raises(ValueError):
        path.file_exists(unknown_file, filenames)

@mock.patch('src.data.path.sys.path')
@mock.patch('src.data.path.os.listdir')
def test_get_filenames (path_mock, listdir_mock):

    path_mock = ['blah']
    directory = ''

    # Raises an error if /home/ hasn't been appended to sys path!
    with pytest.raises(EnvironmentError):
        path.get_filenames(directory)

def test_get_file_path ():

    file = 'testing_file.txt'
    directory = 'dir'
    filenames = ['testing_file.txt']

    known_file_path = f"/home/data/dir/testing_file.txt"

    incorrect_file_name = 'blah'

    with pytest.raises(ValueError):
        path.get_file_path(incorrect_file_name, filenames, directory)

    assert (
        path.get_file_path(file, filenames, directory)
        == known_file_path
    )

def test_get ():

    with pytest.raises(AssertionError):
        path.get(1)
        path.get()

def test_set ():

    with pytest.raises(AssertionError) or pytest.raises(TypeError):
        path.set(1, 1)
        path.set()
        path.set('blah.cha', 'raw')
        path.set('blah.pkl', 'blah')
