# Tensorlakehouse backend implentation of openEO API

The Tensorlakehouse openEO driver is a backend implementation of the [openEO API specification](https://openeo.org/documentation/1.0/developers/api/reference.html). It allows data scientists to list available collections and processes and to submit synchronous and asynchronous requests for data retrieving and data processing

# Table of Contents
- [Tensorlakehouse backend implentation of openEO API](#tensorlakehouse-backend-implentation-of-openeo-api)
- [Table of Contents](#table-of-contents)
  - [User guide](#user-guide)
  - [Python Environment](#python-environment)
  - [Installation](#installation)
  - [Running locally using KIND (Kubernetes In Docker) - RECOMMENDED](#running-locally-using-kind-kubernetes-in-docker---recommended)
  - [Running locally using containers (deprecated)](#running-locally-using-containers-deprecated)
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
  - [Credits](#credits)

## User guide

Please read our [user-guide section](./docs/userguide.md) if you're interested to learn how to use openEO

## Python Environment

Using a virtual environment for all commands in this guide is strongly recommended. In particular, we recommend *python 3.9.16 version*

## Installation

1. Go to `tensorlakehouse-openeo-driver` directory
2. Install *tensorlakehouse-openeo-driver* dependencies: `pip install -r requirements.txt`. Optionally, you can install other dependencies for development purpose: `pip install -r dev_requirements.txt`
3. Optional, but strongly suggested: follow the step described [here](https://github.com/ibm/detect-secrets) to setup detect-secrets tool

## Running locally using KIND (Kubernetes In Docker) - RECOMMENDED
Prerequisites:
- docker/podman installation (e.g., 'docker ps' should run without error)
- kubectl: Follow the [instructions](https://kubernetes.io/docs/tasks/tools/) for your platform
- KIND (Just pick a [release](https://github.com/kubernetes-sigs/kind/releases) for your platform, it is just one binary you need to add to you system's path)
- HELM (Just pick a [release](https://github.com/helm/helm/releases) for your platform, it is just one binary you need to add to you system's path)

Commands:  
- `git clone https://github.com/IBM/tensorlakehouse-openeo-driver.git`  
- `cd tensorlakehouse-openeo-driver/deployments/helm`  
- `./create_kind_cluster.sh`  
- `./deploy_to_kind.sh`  

You can use `watch -n 1 kubectl get po` to follow process on installation. After quite some time, you should see all pods in running or completed state.

This is an example output:
```
dask-worker-f5c5c4896-jwdm4            1/1     Running   0          46h  
openeo-geodn-driver-7f5c6f498c-p8r7w   1/1     Running   0          46h  
pgstac-fastapi-6c8bb56b96-b4jct        1/1     Running   0          46h  
pgstacpostgis-64c49bdfdd-rjpw2         1/1     Running   0          46h  
stac-explorer-7cd65d9bf7-zhzv7         1/1     Running   0          46h  
```

You sould be able to access the following services now:  
dask-scheduler: http://localhost:8787  
openeo-geodn-driver: http://localhost:9091  
pgstac-fastapi: http://localhost:8080  
stac-explorer: http://localhost:8081  
pgstacpostgisservice: http://localhost:5432  


Optionally, you can add some STAC entries using:  
`./init_stac.sh`


## Running locally using containers (deprecated)

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

Each COS instance might have different credentials to access it, so tensorlakehouse uses the bucket name to identify the COS instance by setting environment variables. For instance, if you have a bucket called `my-bucket` that is located in a COS instance called `my-cos-instance`, the environment variables will be:

```shell
MYBUCKET_INSTANCE=my-cos-instance
MYBUCKET_ENDPOINT=s3.us-east.cloud-object-storage.appdomain.cloud
```
In order to define environment variable names, all non-alphanumeric symbols should be removed except for underscore `-`. Thus, `my-bucket` becomes `MYBUCKET`. We need both instance and endpoint variables because a given COS instance might have multiple endpoints. 

Based on this example, we could create a ConfigMap like this:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tensorlakehouse-configmap
  namespace: {{ .Values.namespace }}
data:
  # mapping between COS buckets and instances
  MYBUCKET_INSTANCE: my-cos-instance
  MYBUCKET_ENDPOINT: s3.us-east.cloud-object-storage.appdomain.cloud
```

After mapping buckets to COS instances, we need to set the credentials for each COS instance as environment variables. Each COS instance has a access key and a secret key. The access key is defined as `<cos-instance>_ACCESS_KEY_ID` and the secret key as `<cos-instance>_SECRET_ACCESS_KEY` . Using the same example, the access key would be `MYCOSINSTANCE_ACCESS_KEY` and the secret key `MYCOSINSTANCE_SECRET_KEY` .

This an example of a Kubernetes Secret:

```yaml
kind: Secret
apiVersion: v1
metadata:
  name: tensorlakehouse-openeo-driver-secret
data:
  MYCOSINSTANCE_ACCESS_KEY: <encoded-access-key>
  MYCOSINSTANCE_SECRET_KEY: <encoded-secret>
```

If it is not clear how to define the environment variable names based on bucket names, this is the python function that converts bucket names to environment variable names by removing invalid characters.

```python
def remove_invalid_characters(name: str) -> str:
    """environment variables must have alpha-numeric characters and underscore. This function
    remove what is invalid

    Args:
        name (str): name of the bucket or instance

    Returns:
        str: core part of env var
    """
    assert isinstance(name, str), f"Error! {name=} is not a str"
    env_var = "".join([i if str.isalnum(i) or i == "_" else "" for i in name])
    return env_var

```


#### *Step 2.* Set the environment variables and create  `.env` file
```
# credentials to access cloud object store 
MYCOSINSTANCE_ACCESS_KEY=my-access-key
MYCOSINSTANCE_SECRET_KEY=my-secret-key 

MYBUCKET_INSTANCE=my-cos-instance
MYBUCKET_ENDPOINT=s3.us-east.cloud-object-storage.appdomain.cloud

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

## Credits

TensorLakeHouse is supported by the EU’s Horizon Europe program under Grant Agreement number 101131841 and also received funding from the Swiss State Secretariat for Education, Research and Innovation (SERI) and the UK Research and Innovation (UKRI).

