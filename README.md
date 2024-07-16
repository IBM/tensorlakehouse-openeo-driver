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
      - [*Step 1* Generate credentials](#step-1-generate-credentials)
      - [*Step 2.* Set the environment variables and create  `.env` file](#step-2-set-the-environment-variables-and-create--env-file)
      - [*Step 3* - Build tensorlakehouse-openeo-driver](#step-3---build-tensorlakehouse-openeo-driver)
      - [*Step 4* - Run services using podman-compose](#step-4---run-services-using-podman-compose)
  - [Setup Broker and Result store](#setup-broker-and-result-store)
  - [Software architecture](#software-architecture)
  - [Contributing](#contributing)
  - [Getting support](#getting-support)

## User guide

Please read our [user-guide section](./docs/userguide.md) if you're interested to learn how to use openEO

## Python Environment

Using a virtual environment for all commands in this guide is strongly recommended. In particular, we recommend *python 3.9.16 version*

## Installation

1. Go to `tensorlakehouse-openeo-driver` directory
2. Install *tensorlakehouse-openeo-driver* dependencies: `pip install -r requirements.txt`. Optionally, you can install other dependencies for development purpose: `pip install -r dev_requirements.txt`
3. Optional, but strongly suggested: follow the step describe [here](https://github.com/ibm/detect-secrets) to setup detect-secrets tool

## Running locally using containers

### Setting environment varibles:

 - `PYTHONPATH` for instance, `PYTHONPATH=/Users/alice/tensorlakehouse-openeo-driver/`
 - `STAC_URL` URL to the STAC service that you want to connect to (e.g., `https://stac-fastapi-sqlalchemy-nasageospatial-dev.cash.sl.cloud9.ibm.com`)
 - `TLH_<bucket>_*` is a set of credentials that allows this service to access COS S3 buckets
 - `BROKER_URL` - URL to the broker, which mediates communication between clients and workers.
 - `RESULT_BACKEND` - URL to the backend, which is necessary when we want to keep track of the tasks' states or retrieve results from tasks
 - if you want to implement OIDC authentication you need:
   - `APPID_ISSUER`  which is the authorization server url
   - `APPID_USERNAME` username of the authorization server
   - `APPID_PASSWORD` password of the authorization server
   - `OPENEO_AUTH_CLIENT_ID`  client ID
   - `OPENEO_AUTH_CLIENT_SECRET` client secret
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
- postgres database with postgis extension 
- redis database - see [setup redis](#setup-redis)


#### *Step 1* Generate credentials

In order to access a COS bucket, tensorlakehouse needs to set 3 environment variables:
  
* access key id 
* secret access key
* endpoint e.g., `s3.us-south.cloud-object-storage.appdomain.cloud`

Since each bucket might have different credentials to access it, tensorlakehouse uses the bucket name to define the environment variable name. E.g. if you have a bucket called `my-bucket` , the environment variables will be:

* `TLH_MYBUCKET_ACCESS_KEY_ID`
* `TLH_MYBUCKET_SECRET_ACCESS_KEY` 
* `TLH_MYBUCKET_ENDPOINT`

That is, `TLH_` is a prefix for all environment variables and each one has a different suffix: `_ACCESS_KEY_ID` , `_SECRET_ACCESS_KEY` or `_ENDPOINT`. The function that converts a bucket name to the core of the environment variable name is: 
```python
def convert_bucket(bucket: str) -> str:
    env_var = "".join([i.upper() if str.isalnum(i) or i == '_' else "" for i in bucket])
    return env_var
```


#### *Step 2.* Set the environment variables and create  `.env` file
```
# credentials to access cloud object store 
TLH_MYBUCKET_ACCESS_KEY_ID=my-access-key
TLH_MYBUCKET_SECRET_ACCESS_KEY=my-secret-key 
TLH_MYBUCKET_ENDPOINT=s3.us-south.cloud-object-storage.appdomain.cloud

BROKER_URL=<redis database url>
RESULT_BACKEND=<redis database url>

DASK_SCHEDULER_ADDRESS=http://127.0.0.1:8787

### optional environment variables

PYTHONPATH=/Users/alice/tensorlakehouse-openeo-driver/
# basic credential to proprietary solution
GEODN_DISCOVERY_PASSWORD=<geodn-discovery-password>
GEODN_DISCOVERY_USERNAME=<geodn-discovery-username>
# authorization server
APPID_ISSUER=<authorization server url>
# username and password
APPID_USERNAME=<username>
APPID_PASSWORD=<password>
# client id and secret
OPENEO_AUTH_CLIENT_ID=<client id>
OPENEO_AUTH_CLIENT_SECRET=<client secret>

# default is 9091
TENSORLAKEHOUSE_OPENEO_DRIVER_PORT=9091

```

#### *Step 3* - Build tensorlakehouse-openeo-driver

Podman is a drop-in replacement for Docker. If you are a Docker user, just replace `podman` by `docker` and you will be fine. Go to repository root dir and run:
```shell
podman build -t tensorlakehouse-openeo-driver -f Containerfile
```


#### *Step 4* - Run services using podman-compose

 
run podman-compose 

```shell
podman-compose -f podman-compose.yml --env-file /Users/alice/tensorlakehouse-openeo-driver/.env up
```

## Setup Broker and Result store

tensorlakehouse rely on [celery](https://docs.celeryq.dev/en/stable/getting-started/introduction.html) (a distributed task queue) and Redis (broker and result store) to support batch jobs. Once Redis is up and running, you can set the `BROKER_URL` and `RESULT_BACKEND` environment variables so both tensorlakehouse's webserver and worker can connect to it. In this case, both are the same and they have the following format:

```
BROKER_URL=rediss://<username>:<password>@<hostname>:<port>/0?ssl_cert_reqs=none
```

Celery configuration is defined on [celeryconfig.py](./tensorlakehouse_openeo_driver/celeryconfig.py) module. Note that the task routes defined in this module must be the same that are used to run tensorlakehouse worker. In the example below, the task route is `openeo-pgstac-queue`.

```
celery -A tensorlakehouse_openeo_driver.tasks worker -s /opt/app-root/src/tensorlakehouse-openeo-driver/celerybeat-schedule --concurrency 2 --prefetch-multiplier 1 -Ofair -B  -Q openeo-pgstac-queue --loglevel=info
```

## Software architecture

Check [software architecture diagrams](./docs/software-arch.md).

## Contributing

Check [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## Getting support

Check [SUPPORT.md](.github/SUPPORT.md).


