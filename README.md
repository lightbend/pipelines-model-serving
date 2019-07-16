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

The `RecordsReader` class under the `util` project is able to load files from the `CLASSPATH`, a file system (POSIX, not HDFS), and from URLs. There is a truncated data file from 1990, about 1MB in size, in the `data` subproject, but the default configuration in `airline-flights-model-serving-pipeline/src/main/resources/reference.conf/` has entries to pull down many large files from the original URL, storing them locally in the running image for the ingress object. By default, all but one are commented out. You may wish to add a few more, but _if this pod runs out of memory, remove some of them from the list!_

> WARNING: If you decide to load files from the `CLASSPATH` instead, keep in mind that these files are bundled into the application Docker image, so avoid downloading too many of them or the size will be huge!

This application does not attempt to load new model files. The single model is stored in `data/src/main/resources/airline/models` and loaded from the `CLASSPATH` at startup.

## Build and Deploy the Applications

Decide which of the three projects you want to build and deploy, then change to that project in `sbt` and run `buildAndPublish`. If you run any of the following commands in the "root" project (`pipelines-model-serving`), you'll get errors about multiple blueprint files being disallowed.

So, from the `sbt` prompt, do _one_ of the following first:

1. Wine quality: `project wineModelServingPipeline` (corresponding to the directory `wine-model-serving-pipeline`)
2. Airline flights: `project airlineFlightsModelServingPipeline` (corresponding to the directory `airline-flights-model-serving-pipeline`)
3. Recommender: `project recommenderModelServingPipeline` (corresponding to the directory `recommender-model-serving-pipeline`)

Now build. First, you can explicitly verify the blueprint, although this command is also run as part of `buildAndPublish`:

```
verifyBlueprint
```

Build the project:

```
sbt buildAndPublish
```

The correct image name and tag is echoed by this command, which you'll need next. You can also get it in a separate shell window from Docker:

```shell
docker images
```

Deploy the Project, setting `TAG_NAME` for your Docker image and `APP_NAME` to one of following, appropriate for the app you're deploying:

* `wine` for the `wine-model-serving-pipeline` app
* `airline-flights` for the `airline-flights-model-serving-pipeline` app
* `recommender` for the `recommender-model-serving-pipeline` app

Or, just insert the strings in the command.

For the recommender app, use this command

```shell
TAG_NAME=...
APP_NAME=...
kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/${APP_NAME}-model-serving-pipeline:$TAG_NAME
```

For the other two apps, which write records and results to InfluxDB, use this command, where `DB_NAME` should be set to `wine_ml` for the wine app and `airline_flights` for the airline app:

```shell
TAG_NAME=...
APP_NAME=...
DB_NAME=...
kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/${APP_NAME}-model-serving-pipeline:$TAG_NAME \
  raw-egress.InfluxHost="influxdb.influxdb.svc" \
  raw-egress.InfluxPort="8086" \
  raw-egress.InfluxDatabase="$DB_NAME" \
  influx-result-egress.InfluxHost="influxdb.influxdb.svc" \
  influx-result-egress.InfluxPort="8086" \
  influx-result-egress.InfluxDatabase="$DB_NAME"
```

## Notes

The wine data is inspired by this data source:
https://www.kaggle.com/uciml/red-wine-quality-cortez-et-al-2009

The airline data comes from this data set, where you can see the full list of available data files. The airline app pulls some of these files directly from this location at startup:
http://stat-computing.org/dataexpo/2009/the-data.html

Copyright (C) 2019 Lightbend Inc. (https://www.lightbend.com).

