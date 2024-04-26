#!/bin/bash

# gunicorn --config openeo_geodn_driver/gunicorn.conf.py 'openeo_geodn_driver.local_app:create_app()'
# gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b "0.0.0.0:$OPENEO_GEODN_DRIVER_PORT" 'openeo_geodn_driver.local_app:app'
# gunicorn 'openeo_geodn_driver.local_app:create_app()' --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:9091

gunicorn 'openeo_geodn_driver.local_app:create_app()' --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:9091