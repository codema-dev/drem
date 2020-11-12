from pathlib import Path

import pytest
import responses

from requests.exceptions import HTTPError

from drem.utilities.download import Download


@responses.activate
def test_download_task_saves_file_for_valid_request(tmp_path: Path) -> None:
    """Download on valid HTTP request.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "data.zip"
    responses.add(
        responses.GET,
        "http://www.urltodata.ie",
        content_type="application/zip",
        status=200,
    )

    download = Download(
        url="http://www.urltodata.ie", dirpath=str(tmp_path), filename=filename,
    )
    download.run()

    filepath = tmp_path / filename
    assert filepath.exists()


@responses.activate
def test_download_task_raises_error_when_url_not_found(tmp_path: Path) -> None:
    """Raise error if HTTP request is invalid.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "data.zip"
    responses.add(
        responses.GET, "http://www.urltodata.ie", status=404,
    )

    download = Download(
        url="http://www.urltodata.ie", dirpath=tmp_path, filename=filename,
    )
    with pytest.raises(HTTPError):
        download.run()
