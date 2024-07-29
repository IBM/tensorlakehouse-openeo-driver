#!/bin/bash
helm install -f values-kind.yaml kind .
sleep 10
kubectl port-forward service/dask-scheduler 8787:8787 &
kubectl port-forward service/openeo-geodn-driver 9091:9091 &
kubectl port-forward service/pgstac-fastapi 8080:8080 &
kubectl port-forward service/pgstacpostgisservice 5432:5432 &
kubectl port-forward service/stac-explorer 8081:8080 &


