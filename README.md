# Pipelines Machine Learning Examples

This project contains two example pipelines:

1. Judge the quality of wine using models that are served within a streamlet process.
2. Make product recommendations using models that are served _as a service_ using Kubeflow.
3. Predict air traffic delays using an H2O embedded "MOJO" model.

## Setup

### InfluxDB Setup - for Wine Quality Example

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


### Setup Kubeflow - Recommender Example

Kubeflow is used for the recommender example.

Instructions - TBD


### Air Traffic H20 Example

The data files are expected to be found in the `data-ingestors/src/main/resources/airline-data-csv` directory. There is a small file, 10K lines from the 1990 data, `1990-10K.csv`. If you want to use more of the full multi-GB data set, download one or more of the yearly bzip2 files from [this website](http://stat-computing.org/dataexpo/2009/the-data.html) and put them in the `data-ingestors/src/main/resources/airline-data-csv` directory. (You could remove the 10K file to avoid "duplication"...). _Then_ modify `data-ingestors/src/main/resources/application.conf` to
list those files under the `airline-flights` section, following the example shown.

> WARNING: At this time, these files are bundled into the application Docker image,
> so avoid downloading too many of them or the size will be huge! TODO: Have the `AirlineFlightRecordsIngress` download one or more files to a suitable place at run time.

## Build and Deploy ML Pipeline

> **WARNING:** Currently, only the air-traffic example is built. Edit `build.sbt`
> and change the `mainBlueprint` to the desired subproject:
>
> 1. `mainBlueprint := Some("recommender-blueprint.conf")`
> 2. `mainBlueprint := Some("wine-blueprint.conf")`
> 3. `mainBlueprint := Some("airline-flights-blueprint.conf")`

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

