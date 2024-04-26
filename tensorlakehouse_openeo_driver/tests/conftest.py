import contextlib
import io
import logging
import os
import pathlib
import time
from unittest import mock

import flask
import pytest
import pythonjsonlogger.jsonlogger

from openeo_driver.backend import UserDefinedProcesses
from tensorlakehouse_openeo_driver.geodn_backend import GeoDNBackendImplementation
from openeo_driver.server import build_backend_deploy_metadata
from openeo_driver.testing import UrllibMocker
from openeo_driver.util.logging import (
    FlaskUserIdLogging,
    FlaskRequestCorrelationIdLogging,
    BatchJobLoggingFilter,
    LOGGING_CONTEXT_FLASK,
    LOGGING_CONTEXT_BATCH_JOB,
)

from openeo_driver.views import build_app


# pytest_plugins = "pytester"


# conftest.py in the root directory
def pytest_ignore_collect(collection_path: pathlib.Path, path, config):
    """_summary_

    Args:
        collection_path (pathlib.Path): _description_
        path (_type_): _description_
        config (_type_): _description_

    Returns:
        _type_: _description_

    Yields:
        _type_: _description_
    """
    if "openeo-python-driver" in collection_path.parts:
        if "conftest.py" in os.listdir(collection_path):
            return True
    if "libs" in collection_path.parts:
        print(f"ignoring: {collection_path.parts}")
        return True


def pytest_configure(config):
    # Isolate tests from the host machine’s timezone
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="module")
def backend_implementation() -> GeoDNBackendImplementation:
    return GeoDNBackendImplementation()


@pytest.fixture
def udp_registry(backend_implementation) -> UserDefinedProcesses:
    return backend_implementation.user_defined_processes


TEST_APP_CONFIG = dict(
    OPENEO_TITLE="openEO Unit Test Dummy Backend",
    TESTING=True,
    SERVER_NAME="oeo.net",
    OPENEO_BACKEND_DEPLOY_METADATA=build_backend_deploy_metadata(
        packages=["openeo", "openeo_driver"]
    ),
)


@pytest.fixture(scope="module")
def flask_app(backend_implementation) -> flask.Flask:
    app = build_app(
        backend_implementation=backend_implementation,
        # error_handling=False,
    )
    app.config.from_mapping(TEST_APP_CONFIG)
    return app


@pytest.fixture
def client(flask_app):
    return flask_app.test_client()


@pytest.fixture
def urllib_mock() -> UrllibMocker:
    with UrllibMocker().patch() as mocker:
        yield mocker


@contextlib.contextmanager
def enhanced_logging(
    level=logging.INFO,
    json=False,
    format=None,
    request_ids=("123-456", "234-567", "345-678", "456-789", "567-890"),
    context=LOGGING_CONTEXT_FLASK,
):
    """Set up logging with additional injection of request id, user id, ...."""
    root_logger = logging.getLogger()
    orig_root_level = root_logger.level

    out = io.StringIO()
    handler = logging.StreamHandler(out)
    handler.setLevel(level)
    if json:
        formatter = pythonjsonlogger.jsonlogger.JsonFormatter(format)
    else:
        formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    if context == LOGGING_CONTEXT_FLASK:
        handler.addFilter(FlaskRequestCorrelationIdLogging())
        handler.addFilter(FlaskUserIdLogging())
    elif context == LOGGING_CONTEXT_BATCH_JOB:
        handler.addFilter(BatchJobLoggingFilter())
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
    try:
        with mock.patch.object(
            FlaskRequestCorrelationIdLogging,
            "_build_request_id",
            side_effect=request_ids,
        ):
            yield out
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(orig_root_level)


# def pytest_ignore_collect(path, config):
#     # suppose our condition is some command line argument is passed in
#     val = config.getvalue("-k")
#     if val == "":
#         here = pathlib.Path.cwd().absolute()
#         skip_fd = here / "libs"
#         if skip_fd == path:
#             return True
