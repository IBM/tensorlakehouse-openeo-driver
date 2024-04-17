"""gunicorn WSGI server configuration."""
from multiprocessing import cpu_count
from openeo_geodn_driver.constants import OPENEO_GEODN_DRIVER_PORT


def max_workers():
    return cpu_count()


bind = f"0.0.0.0:{OPENEO_GEODN_DRIVER_PORT}"
max_requests = 1000
timeout = 2700
worker_class = "gevent"
workers = max_workers()
# workers = 4
