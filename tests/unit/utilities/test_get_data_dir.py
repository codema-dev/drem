from drem.utilities.get_data_dir import get_data_dir


def test_get_data_dir_is_correct():
    """Test data directory path is correct."""
    assert get_data_dir() == "/drem/data"
