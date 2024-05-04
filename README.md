# Tensorlakehouse backend implentation of openEO API

The Tensorlakehouse openEO driver is a backend implementation of the [openEO API specification](https://openeo.org/documentation/1.0/developers/api/reference.html). It allows data scientists to list available collections and processes and to submit synchronous and asynchronous requests for data retrieving and data processing

# Table of Contents
- [Tensorlakehouse backend implentation of openEO API](#tensorlakehouse-backend-implentation-of-openeo-api)
- [Table of Contents](#table-of-contents)
  - [User guide](#user-guide)
  - [Python Environment](#python-environment)
  - [Installation](#installation)
  - [Running locally using containers](#running-locally-using-containers)
    - [Setting environment varibles:](#setting-environment-varibles)
    - [Building and running container images](#building-and-running-container-images)
  - [Contributing](#contributing)
  - [Getting support](#getting-support)

## User guide

Please read our [user-guide section](./docs/userguide.md) if you're interested to learn how to use openEO

## Python Environment

Using a virtual environment for all commands in this guide is strongly recommended. In particular, we recommend *python 3.9.16 version*

## Installation

1. Go to `tensorlakehouse-openeo-driver` directory
2. Install *tensorlakehouse-openeo-driver* dependencies: `pip install -r requirements.txt`. Optionally, you can install other dependencies for development purpose: `pip install -r dev_requirements.txt`
3. Optional, but strongly suggested: follow the step describe [here](https://w3.ibm.com/w3publisher/detect-secrets) to setup detect-secrets tool

## Running locally using containers


### Setting environment varibles:

 - `PYTHONPATH` for instance, `PYTHONPATH=/Users/alice/tensorlakehouse-openeo-driver/`
 - `STAC_URL` URL to the STAC service that you want to connect to (e.g., `https://stac-fastapi-sqlalchemy-nasageospatial-dev.cash.sl.cloud9.ibm.com`)
 - `CREDENTIALS` is a set of credentials (encoded in base64) that allows this service to access COS S3 buckets
 - `BROKER_URL` - URL to the broker, which mediates communication between clients and workers.
 - `RESULT_BACKEND` - URL to the backend, which is necessary when we want to keep track of the tasks' states or retrieve results from tasks
 - `GEODN_DISCOVERY_USERNAME` and `GEODN_DISCOVERY_PASSWORD` (optional) for basic auth to get GeoDN.Discovery (former PAIRS) metadata

`FLASK_APP` and `FLASK_DEBUG` environment variables are useful for debugging:

```shell
cd <path-to-parent-dir>/tensorlakehouse-openeo-driver/
export FLASK_APP=tensorlakehouse_openeo_driver.local_app
export FLASK_DEBUG=1
flask run
```

### Building and running container images

Prerequisites: 
- docker or podman-compose installed
- 

Podman is a drop-in replacement for Docker. If you are a Docker user, just replace `podman` by `docker` and you will be fine. 

```shell
podman-compose -f podman-compose.yml --env-file /Users/alice/tensorlakehouse-openeo-driver/.env up
```

Create `.env` file

```

PYTHONPATH=/Users/alice/tensorlakehouse-openeo-driver/

# RIS3 account - App ID-geodn instance - resource group foc-cimf
APPID_ISSUER=<authorization server url>
APPID_USERNAME=<username>
APPID_PASSWORD=<password>
OPENEO_AUTH_CLIENT_ID=<client id>
OPENEO_AUTH_CLIENT_SECRET=<client secret>


TENSORLAKEHOUSE_OPENEO_DRIVER_PORT=9091


GEODN_DISCOVERY_PASSWORD=<geodn-discovery-password>
GEODN_DISCOVERY_USERNAME=<geodn-discovery-username>


DASK_SCHEDULER_ADDRESS=http://127.0.0.1:8787


STAC_URL=https://stac-fastapi-pgstac-geospatial-be-staging.apps.fmaas-backend.fmaas.res.ibm.com/
OPENEO_URL=https://openeo-geodn-driver-pgstac-geospatial-be-staging.apps.fmaas-backend.fmaas.res.ibm.com/openeo/1.1.0/

OPENEO_USERNAME=john

OPENEO_PASSWORD=john123

CREDENTIALS=

BROKER_URL=<
RESULT_BACKEND=



# NUMEXPR_MAX_THREADS=8
```

## Contributing

Check [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## Getting support

Check [SUPPORT.md](.github/SUPPORT.md).


