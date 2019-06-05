# Pipelines Machine Learning Examples

This project contains two example pipelines:

1. Judge the quality of wine using models that are served within a streamlet process.
2. Make product recommendations using models that are served _as a service_ using Kubeflow.

## Setup

### InfluxDB Setup

Wine scoring results are written to InfluxDB, as an example of a downstream consumer. If you don't want to setup Influx, 
change `wineblueprint.conf` to remove the `influx-raw-egress.in` and `influx-result-egress.in` from the `connections`
section of the blueprint.

First Install the Influx DB CLI
> brew install influxdb

Make sure you are connected to the kubernetes cluster and run the command below to install InfluxDB
> helm install stable/influxdb --name influxdb --namespace influxdb

Port forward to access InfluxDB locally 
> kubectl port-forward --namespace influxdb $(kubectl get pods --namespace influxdb -l app=influxdb -o jsonpath='{ .items[0].metadata.name }') 8086:8086

Connect to influxDB and create Database
> influx -execute 'create database wine_ml' -host localhost -port 8086


### Setup Kubeflow

Kubeflow is used for the recommender example.

Instructions - TBD

### Build and Deploy Ml Pipeline

Verify blueprint:

```
sbt verifyBlueprint
```

Build the project:

```
sbt buildAndPublish
```

Get the image name and tag:

```
docker images
```

Deploy the Project, setting `TAG_NAME` for your Docker image:

```
TAG_NAME=...
kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/model-serving-pipeline:$TAG_NAME

kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/ml-serving-pipeline:$TAG_NAME \
  raw-egress.InfluxHost="influxdb.influxdb.svc" \
  raw-egress.InfluxPort="8086" \
  raw-egress.InfluxDatabase="wine_ml" \
  influx-result-egress.InfluxHost="influxdb.influxdb.svc" \
  influx-result-egress.InfluxPort="8086" \
  influx-result-egress.InfluxDatabase="wine_ml"
```

## Notes

The wine data is inspired by this data source: 
https://www.kaggle.com/uciml/red-wine-quality-cortez-et-al-2009

Copyright (C) 2019 Lightbend Inc. (https://www.lightbend.com).

