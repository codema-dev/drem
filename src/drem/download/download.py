from pathlib import Path
from typing import Any
from typing import Optional

import requests

from prefect import Task
from tqdm import tqdm


def download_file_from_response(response: requests.Response, filepath: Path) -> None:
    """Download file to filepath via a HTTP response from a POST or GET request.

    Args:
        response (requests.Response): A HTTP response from a POST or GET request
        filepath (Path): Save path destination for downloaded file
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
        self, url: Optional[str] = None, **kwargs: Any,
    ):
        """Initialise 'Download' Task.

        Args:
            url (Optional[str], optional): Direct URL link to data such as
                http://www.urltodata.ie/data.csv. Defaults to None.
            **kwargs (Any): see  https://docs.prefect.io/core/concepts/tasks.html
        """
        self.url = url

        super().__init__(**kwargs)

    def run(self, savedir: Path, filename: str, file_extension: str) -> None:
        """Download data directly from URL to savepath.

        Args:
            savedir (Path): Path to save directory.
            filename (str): Name of file
            file_extension (str): File extension (such as '.csv')
        """
        savepath = savedir / f"{filename}{file_extension}"

        if not savepath.exists():
            with requests.get(url=self.url, stream=True) as response:

                response.raise_for_status()
                download_file_from_response(response, savepath)
