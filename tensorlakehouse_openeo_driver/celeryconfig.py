# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from openeo_geodn_driver.constants import (
    result_backend,
    broker_url,
)

result_backend = result_backend
broker_url = broker_url
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
task_track_started = True
task_routes = {"*": {"queue": "openeo-pgstac-queue"}}
CELERY_TASK_ACKS_LATE = True
