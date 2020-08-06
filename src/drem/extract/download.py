#!/usr/bin/env python

from pathlib import Path

import requests
from tqdm import tqdm


def _download_file_from_response(response: requests.Response, filepath: Path) -> None:

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

    Parameters
    ----------
    url : str
        url linking to data to be downloaded; can be found by right-clicking
        the download link and selecting 'Copy Link Location'
    filepath : Path
        Save destination for data
    """
    with requests.get(url=url, stream=True) as response:

        response.raise_for_status()
        _download_file_from_response(response, filepath)
