FROM base-tensorlakehouse-openeo-driver:latest

LABEL "name"="base-tensorlakehouse-openeo-driver"
LABEL "vendor"="IBM"
LABEL "summary"="Base image of Backend implementation of openEO API"

WORKDIR ${TENSORLAKEHOUSE_OPENEO_DRIVER_DIR}

# copy app
COPY tensorlakehouse_openeo_driver tensorlakehouse_openeo_driver
COPY libs libs
COPY logging.conf .
COPY pyproject.toml .
COPY setup.py .

# setup UTF-8 locale
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV PORT=9091
EXPOSE ${PORT}

CMD bash tensorlakehouse_openeo_driver/run_gunicorn.sh
