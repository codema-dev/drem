# flake8: noqa

import logging

import pytest

from _pytest.logging import caplog as _caplog
from _pytest.monkeypatch import MonkeyPatch
from loguru import logger


@pytest.fixture(autouse=True)
def no_http_requests(monkeypatch):
    """Prevent any test from making HTTP requests.

    Source: https://blog.jerrycodes.com/no-http-requests/

    Args:
        monkeypatch (MonkeyPatch): [description]
    """

    def urlopen_mock(self, method, url, *args, **kwargs):
        raise RuntimeError(
            f"The test was about to {method} {self.scheme}://{self.host}{url}",
        )

    monkeypatch.setattr(
        "urllib3.connectionpool.HTTPConnectionPool.urlopen", urlopen_mock,
    )


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    logger.remove(handler_id)
