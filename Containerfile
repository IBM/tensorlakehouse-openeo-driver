FROM docker.io/python:3.9
# setup
LABEL "name"="tensorlakehouse_openeo_driver"
LABEL "vendor"="IBM"
LABEL "summary"="Backend implementation of openEO API"
# https://distributed.dask.org/en/latest/worker-memory.html#automatically-trim-memory
ENV MALLOC_TRIM_THRESHOLD_=0
# set path to openeo-geodn-driver
ENV TENSORLAKEHOUSE_OPENEO_DRIVER_DIR=/opt/app-root/src/tensorlakehouse-openeo-driver

# path to openeo-geodn-driver data
ENV TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR=${TENSORLAKEHOUSE_OPENEO_DRIVER_DIR}/data

# set path to dataservice-sdk
ENV DATASERVICE_PATH=${TENSORLAKEHOUSE_OPENEO_DRIVER_DIR}/libs/dataservice_sdk/src

# set pythonpath that includes dataservice_sdk
ENV PYTHONPATH=${TENSORLAKEHOUSE_OPENEO_DRIVER_DIR}:${DATASERVICE_PATH}

RUN echo ${PYTHONPATH}

RUN mkdir -p ${TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR}

USER root
# based on https://stackoverflow.com/questions/58473832/how-do-i-change-the-permissions-in-openshift-container-platform/58474145#58474145
RUN chgrp -R 0 ${TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR} && \
    chmod -R g=u ${TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR}

WORKDIR ${TENSORLAKEHOUSE_OPENEO_DRIVER_DIR}
# update pip
RUN python3 -m pip install -U pip

# install requirements
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# copy app
COPY tensorlakehouse_openeo_driver tensorlakehouse_openeo_driver
#COPY libs libs
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

CMD bash tensorlakehouse_openeo_driver/run_gunicorn.sh

# ENV FLASK_APP=tensorlakehouse_openeo_driver.local_app
# ENV FLASK_DEBUG=0
# CMD flask run --host=0.0.0.0 --port=${PORT}
# CMD python3 tensorlakehouse_openeo_driver/local_app.py
