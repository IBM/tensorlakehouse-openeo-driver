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
  - [Credits](#credits)

## User guide

Please read our [user-guide section](./docs/userguide.md) if you're interested to learn how to use openEO

## Python Environment

Using a virtual environment for all commands in this guide is strongly recommended. In particular, we recommend *python 3.9.16 version*

## Installation

tensorlakehouse backend relies on [openeo-python-driver](https://github.com/Open-EO/openeo-python-driver) to implement request handling

1. Clone this repository and its submodules: `git clone --recurse-submodules https://github.com/IBM/tensorlakehouse-openeo-driver`
2. Go to `tensorlakehouse-openeo-driver` directory
3. Install *tensorlakehouse-openeo-driver* dependencies: `pip install -r requirements.txt`. Optionally, you can install other dependencies for development purpose: `pip install -r dev_requirements.txt`
4. Optional, but strongly suggested: follow the step describe [here](https://w3.ibm.com/w3publisher/detect-secrets) to setup detect-secrets tool

## Running locally using containers


### Setting environment varibles:

 - `PYTHONPATH` should have 3 values separated by colons. For instance, `PYTHONPATH=/Users/alice/tensorlakehouse-openeo-driver/:/Users/alice/tensorlakehouse-openeo-driver/libs/openeo-python-driver:/Users/alice/tensorlakehouse-openeo-driver/libs/dataservice_sdk/src`. This variable is composed by:
     *. full path to `tensorlakehouse-openeo-driver` directory
     *. full path to `openeo-python-driver` directory, which is a submodule of this repository
     *. full path to `dataservice_sdk` directory, which is a submodules of this repository
 - `STAC_URL` URL to the STAC service that you want to connect to (e.g., `https://stac-fastapi-sqlalchemy-nasageospatial-dev.cash.sl.cloud9.ibm.com`)
 - `CREDENTIALS` is a set of credentials (encoded in base64) that allows this service to access COS S3 buckets
 - `BROKER_URL` - URL to the broker, which mediates communication between clients and workers.
 - `RESULT_BACKEND` - URL to the backend, which is necessary when we want to keep track of the tasks' states or retrieve results from tasks
 - `GEODN_DISCOVERY_USERNAME` and `GEODN_DISCOVERY_PASSWORD` (optional) for basic auth to get GeoDN.Discovery (former PAIRS) metadata

`FLASK_APP` and `FLASK_DEBUG` environment variables are useful for debugging:

```shell
cd <path-to-parent-dir>/tensorlakehouse-openeo-driver/
export FLASK_APP=openeo_geodn_driver.local_app
export FLASK_DEBUG=1
flask run
```

### Building and running container images

Prerequisites: STAC service and Redis DB are up and running. If you want to use Dask Scheduler, then it must also be up and running.

Podman is a drop-in replacement for Docker. If you are a Docker user, just replace `podman` by `docker` and you will be fine. 

```shell
podman build -t tensorlakehouse-openeo-driver .
```

Run openEO webserver

```shell
podman run --rm -p 9091:9091 -e GEODN_DISCOVERY_USERNAME=$GEODN_DISCOVERY_USERNAME -e GEODN_DISCOVERY_PASSWORD=$GEODN_DISCOVERY_PASSWORD -e STAC_URL=$STAC_URL -e OPENEO_URL=$OPENEO_URL -e CREDENTIALS=$CREDENTIALS -e BROKER_URL=$BROKER_URL -e RESULT_BACKEND=$RESULT_BACKEND tensorlakehouse-openeo-driver
```

Run openEO celery worker
```shell
podman run --rm -e GEODN_DISCOVERY_USERNAME=$GEODN_DISCOVERY_USERNAME -e GEODN_DISCOVERY_PASSWORD=$GEODN_DISCOVERY_PASSWORD -e STAC_URL=$STAC_URL -e OPENEO_URL=$OPENEO_URL -e CREDENTIALS=$CREDENTIALS -e BROKER_URL=$BROKER_URL -e RESULT_BACKEND=$RESULT_BACKEND tensorlakehouse-openeo-driver
```

## Contributing

Check [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## Getting support

Check [SUPPORT.md](.github/SUPPORT.md).

## Credits

This project was created using <https://github.ibm.com/BiomedSciAI-Innersource/python-blueprint>.

