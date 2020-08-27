# flake8: noqa

import pytest

from _pytest.monkeypatch import MonkeyPatch
from tdda.referencetest import referencepytest


def pytest_addoption(parser):
    referencepytest.addoption(parser)


def pytest_collection_modifyitems(session, config, items):
    referencepytest.tagged(config, items)


@pytest.fixture(scope="module")
def ref(request):
    return referencepytest.ref(request)


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
