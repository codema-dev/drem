from os import path
from pathlib import Path
from typing import Any

import requests

from prefect import Task
from tqdm import tqdm


def download_file_from_response(response: requests.Response, filepath: str) -> None:
    """Download file to filepath via a HTTP response from a POST or GET request.

    Args:
        response (requests.Response): A HTTP response from a POST or GET request
        filepath (str): Save path destination for downloaded file
    """
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kilobyte
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)

    with open(filepath, "wb") as save_destination:

        for stream_data in response.iter_content(block_size):
            progress_bar.update(len(stream_data))
            save_destination.write(stream_data)

    progress_bar.close()


def download(url: str, filepath: Path) -> None:
    """Download a file from url to filepath.

    If no filepath is entered the file name will be inferred from the url
    and saved to the 'external' dir in the data directory.

    Args:
        url (str): url linking to data to be downloaded
        filepath (Path): Save destination for data
    """
    with requests.get(url=url, stream=True) as response:

        response.raise_for_status()
        download_file_from_response(response, filepath)


def _raise_for_empty_arguments(*args):

    empty_args = [arg for arg in args if arg is None]

    if any(empty_args) is None:
        raise ValueError(f"At least one of {args} is None.")


class Download(Task):
    """Download file directly from URL.

    Example:
    Save `data.csv` to current working directory:
    ```python
    download_data = Download(
        url=http://www.urltodata.ie/data.csv, savepath='data.csv',
    )
    download_data.run()
    ```

    Args:
        Task (Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def __init__(
        self, url: str, dirpath: str, filename: str, **kwargs: Any,
    ):
        """Initialise 'Download' Task.

        Args:
            url (str): Direct URL link to data such as
                http://www.urltodata.ie/data.csv. Defaults to None.
            filename (str): Name of file to be downloaded
            dirpath (str): Path to save directory
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.url = url
        self.dirpath = dirpath
        self.filename = filename

        super().__init__(**kwargs)

    def run(self) -> str:
        """Download data directly from URL to savepath.

        Returns:
            str: Path to downloaded data
        """
        savepath = path.join(self.dirpath, self.filename)
        if path.exists(savepath):
            self.logger.info(f"Skipping download as {savepath} exists!")
        else:
            with requests.get(url=self.url, stream=True) as response:
                response.raise_for_status()
                download_file_from_response(response, savepath)

        return savepath


if __name__ == "__main__":

    download_codema_homepage = Download(
        name="Download the Codema homepage",
        url="https://www.codema.ie/",
        filename="codema-homepage.html",
        dirpath="/drem/data/roughwork",
    )
    download_codema_homepage.run()
