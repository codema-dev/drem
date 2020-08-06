from logging import Logger
from pathlib import Path
import re

import requests
from tqdm import tqdm
from prefect import Task

from drem._filepaths import EXTERNAL_DIR


def _download_file_from_response(
    response: requests.Response, filepath: Path, logger: Logger
) -> None:

    if logger:
        logger.info(f"\nDownloading file to {filepath} ...")
    else:
        print(f"\nDownloading file to {filepath} ...")

    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kilobyte
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)

    with open(filepath, "wb") as file:

        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)

    progress_bar.close()


def download(url: str, filepath: Path = None) -> None:

    if not filepath:
        filename = re.findall(r"/([\w.]+)$", url)[0]
        filepath = EXTERNAL_DIR / filename

    with requests.get(url=url, stream=True) as response:

        response.raise_for_status()
        _download_file_from_response(response, filepath, logger)
