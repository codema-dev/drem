from pathlib import Path

import pytest

from drem.download.download import download


def test_task_http_request_is_blocked(tmp_path: Path) -> None:
    """No_http_requests fixture in conftest blocks http requests.

    Args:
        tmp_path (Path): A Pytest plugin to create a temporary path
    """
    with pytest.raises(RuntimeError):
        download("http://www.url-to-some-file.com", tmp_path)
