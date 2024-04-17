FROM registry.cirrus.ibm.com/public/python-39:latest
# setup
LABEL "name"="openeo_geodn_driver"
LABEL "vendor"="IBM"
LABEL "summary"="OpenEO GeoDN driver"
# https://distributed.dask.org/en/latest/worker-memory.html#automatically-trim-memory
ENV MALLOC_TRIM_THRESHOLD_=0
# PYTHONPATH=libs/openeo-python-driver:/Users/ltizzei/Projects/Orgs/geodn-discovery/openeo-geodn-driver/:/Users/ltizzei/Projects/Orgs/geodn-discovery/openeo-geodn-driver/libs/dataservice_sdk/src
# set path to openeo-geodn-driver
ENV OPENEO_GEODN_DRIVER_DIR=/opt/app-root/src/openeo-geodn-driver

# path to openeo-geodn-driver data
ENV OPENEO_GEODN_DRIVER_DATA_DIR=${OPENEO_GEODN_DRIVER_DIR}/data

# set path to dataservice-sdk
ENV DATASERVICE_PATH=${OPENEO_GEODN_DRIVER_DIR}/libs/dataservice_sdk/src

# set path to openeo-python-driver
ENV OPENEO_PYTHON_DRIVER=${OPENEO_GEODN_DRIVER_DIR}/libs/openeo-python-driver
# set pythonpath that includes openeo-python-driver and dataservice_sdk
ENV PYTHONPATH=${OPENEO_GEODN_DRIVER_DIR}:${DATASERVICE_PATH}:${OPENEO_PYTHON_DRIVER}

RUN echo ${PYTHONPATH}
# create directories
# RUN mkdir -p ${OPENEO_GEODN_DRIVER_DIR}

RUN mkdir -p ${OPENEO_GEODN_DRIVER_DATA_DIR}

USER root
# based on https://stackoverflow.com/questions/58473832/how-do-i-change-the-permissions-in-openshift-container-platform/58474145#58474145
RUN chgrp -R 0 ${OPENEO_GEODN_DRIVER_DATA_DIR} && \
    chmod -R g=u ${OPENEO_GEODN_DRIVER_DATA_DIR}

WORKDIR ${OPENEO_GEODN_DRIVER_DIR}
# update pip
RUN python3 -m pip install -U pip

# install requirements
COPY requirements.txt .
COPY dev_requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt
RUN python3 -m pip install --no-cache-dir -r dev_requirements.txt
# copy app
COPY openeo_geodn_driver openeo_geodn_driver
COPY libs libs
COPY logging.conf .
COPY pyproject.toml .
COPY setup.py .

# RUN python3 -m pip install --no-cache-dir -e ".[test]"

# setup UTF-8 locale
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV PORT=9091
EXPOSE ${PORT}

CMD bash openeo_geodn_driver/run_gunicorn.sh

# ENV FLASK_APP=openeo_geodn_driver.local_app
# ENV FLASK_DEBUG=0
# CMD flask run --host=0.0.0.0 --port=${PORT}
# CMD python3 openeo_geodn_driver/local_app.py