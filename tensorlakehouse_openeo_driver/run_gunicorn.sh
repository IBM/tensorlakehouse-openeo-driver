#!/bin/bash

# gunicorn --config tensorlakehouse_openeo_driver/gunicorn.conf.py 'tensorlakehouse_openeo_driver.local_app:create_app()'
# gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b "0.0.0.0:$tensorlakehouse_openeo_driver_PORT" 'tensorlakehouse_openeo_driver.local_app:app'
# gunicorn 'tensorlakehouse_openeo_driver.local_app:create_app()' --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:9091

gunicorn 'tensorlakehouse_openeo_driver.local_app:create_app()' --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:9091