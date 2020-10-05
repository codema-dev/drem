from pathlib import Path

import pytest
import responses

from requests.exceptions import HTTPError

from drem.download.download import Download


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
    savepath = tmp_path / "data.zip"
    download = Download(url="http://www.urltodata.ie", savepath=savepath)

    download.run()

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
    savepath = tmp_path / "data.zip"
    download = Download(url="http://www.urltodata.ie", savepath=savepath)

    with pytest.raises(HTTPError):
        download.run()


@pytest.mark.parametrize("url", [None, "http://www.urltodata.ie"])
@pytest.mark.parametrize("savepath", [None, None])
def test_download_task_raises_error_if_args_are_empty(
    tmp_path: Path, url: str, savepath: Path,
) -> None:
    """Raise error when no URL or savepath is defined.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
        url (str): URL to data
        savepath (Path): Data savepath
    """
    download = Download()

    with pytest.raises(ValueError):
        download.run()
