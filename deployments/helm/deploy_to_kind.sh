#!/bin/bash
helm install -f values-kind.yaml kind .
(while true; do  kubectl port-forward service/dask-scheduler 8787:8787; done) &
(while true; do  kubectl port-forward service/openeo-geodn-driver 9091:9091; done) &
(while true; do  kubectl port-forward service/pgstac-fastapi 8080:8080; done) &
(while true; do  kubectl port-forward service/pgstacpostgisservice 5432:5432; done) &
(while true; do  kubectl port-forward service/stac-explorer 8081:8080; done) &



