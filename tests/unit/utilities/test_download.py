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
    responses.add(
        responses.GET,
        "http://www.urltodata.ie",
        content_type="application/zip",
        status=200,
    )
    savedir = tmp_path
    filename = "data"
    file_extension = ".zip"
    download = Download(url="http://www.urltodata.ie")

    download.run(
        savedir=savedir, filename=filename, file_extension=file_extension,
    )

    savepath = savedir / f"{filename}.{file_extension}"
    assert savepath.exists()


@responses.activate
def test_download_task_raises_error_when_url_not_found(tmp_path: Path) -> None:
    """Raise error if HTTP request is invalid.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    responses.add(
        responses.GET, "http://www.urltodata.ie", status=404,
    )
    savedir = tmp_path
    filename = "data"
    file_extension = ".zip"
    download = Download(url="http://www.urltodata.ie")

    with pytest.raises(HTTPError):
        download.run(
            savedir=savedir, filename=filename, file_extension=file_extension,
        )
