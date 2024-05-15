"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import os
import sys
from asgiref.wsgi import WsgiToAsgi
from dask.distributed import Client, LocalCluster

import openeo_driver
from tensorlakehouse_openeo_driver.tensorlakehouse_backend import TensorLakeHouseBackendImplementation

# from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import get_logging_config, setup_logging, show_log_level
from openeo_driver.views import OpenEoApiApp, build_app
from tensorlakehouse_openeo_driver.constants import (
    DASK_SCHEDULER_ADDRESS,
    TENSORLAKEHOUSE_OPENEO_DRIVER_PORT,
    STAC_URL,
)

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


def make_dask_client() -> Client:
    """instantiate dask client.

    Returns:
        Client: dask client
    """

    client = None
    # if remote dask scheduler is available
    if DASK_SCHEDULER_ADDRESS is not None and "0.0.0.0" in DASK_SCHEDULER_ADDRESS:
        # otherwise use local dask cluster
        cluster = LocalCluster(n_workers=4)
        logger.debug(f"\n\nDask dashboard link={cluster.dashboard_link}")
        client = Client(cluster)
        logger.debug("Connecting to local dask scheduler")
    else:
        client = Client(
            DASK_SCHEDULER_ADDRESS,
            scheduler_options={"distributed.scheduler.debug": True},
        )
        logger.debug(f"Connecting to remote dask scheduler: {DASK_SCHEDULER_ADDRESS}")

    assert client is not None
    return client


def create_app(environment: str = "production") -> OpenEoApiApp:
    # "create_app" factory for Flask Application discovery
    # see https://flask.palletsprojects.com/en/2.1.x/cli/#application-discovery
    logger.debug(
        f"Starting openeo-geodn-driver - Env vars: \nSTAC_URL={STAC_URL} \
        \nDASK_SCHEDULER_ADDRESS={DASK_SCHEDULER_ADDRESS}"
    )
    assert environment.lower() in [
        "dev",
        "production",
    ], f"Error! Invalid environment: {environment}"
    app = build_app(backend_implementation=TensorLakeHouseBackendImplementation())
    app.config.from_mapping(
        OPENEO_TITLE="GeoDN Backend compliant with OpenEO",
        OPENEO_DESCRIPTION="GeoDN Backend compliant with OpenEO",
        OPENEO_BACKEND_VERSION=openeo_driver.__version__,
    )
    if environment.lower() == "dev":
        return app
    else:
        # support ASGI server https://flask.palletsprojects.com/en/2.0.x/deploying/asgi/
        asgi_app = WsgiToAsgi(app)
        return asgi_app


if __name__ == "__main__":
    setup_logging(
        get_logging_config(
            # root_handlers=["stderr_json"],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "flask": {"level": "DEBUG"},
                "werkzeug": {"level": "DEBUG"},
                "kazoo": {"level": "WARN"},
                "gunicorn": {"level": "INFO"},
            },
        )
    )
    logger.info(
        repr(
            {
                "pid": os.getpid(),
                "interpreter": sys.executable,
                "version": sys.version,
                "argv": sys.argv,
            }
        )
    )

    app = create_app(environment="dev")
    show_log_level(app.logger)
    host = "0.0.0.0"
    port = int(TENSORLAKEHOUSE_OPENEO_DRIVER_PORT)
    debug = os.getenv("FLASK_DEBUG", False)
    app.run(host=host, port=port, debug=debug)
    # print(f"Running gunicorn {host}:{port}")
    # run_gunicorn(app=app, threads=4, host=host, port=port)
