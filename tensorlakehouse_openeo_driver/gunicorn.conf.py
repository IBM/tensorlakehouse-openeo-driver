"""gunicorn WSGI server configuration."""

from multiprocessing import cpu_count
from tensorlakehouse_openeo_driver.constants import (
    TENSORLAKEHOUSE_OPENEO_DRIVER_PORT,
    TENSORLAKEHOUSE_OPENEO_DRIVER_HOST,
)


def max_workers():
    return cpu_count()


bind = f"{TENSORLAKEHOUSE_OPENEO_DRIVER_HOST}:{TENSORLAKEHOUSE_OPENEO_DRIVER_PORT}"
max_requests = 1000
timeout = 2700
worker_class = "gevent"
workers = max_workers()
# workers = 4
