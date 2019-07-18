# Pipelines Machine Learning Examples

This project contains two example pipelines:

1. Judge the quality of wine using models that are served within a streamlet process.
2. Make product recommendations using models that are served _as a service_ using Kubeflow.
3. Predict air traffic delays using an H2O embedded "MOJO" model.

## Setup

### InfluxDB Setup - for Wine Quality and Airline Flights Examples

> NOTE: At this time, the _egress_ streamlets that write to InfluxDB are not used in the corresponding blueprints for the wine and airline apps. If you want to use Influx, edit those blueprint files, uncommenting the lines that use these egresses and commenting the corresponding lines that don't.

Wine input records and scoring results are written to InfluxDB, as an example of a downstream consumer. Similarly for the Airline flights app.

You can enable or disable the corresponding egress streamlets by editing the corresponding `blueprint.conf` to add or remove the `influx-raw-egress.in` and `influx-result-egress.in` from the `connections` section of the blueprint.

First Install the Influx DB CLI. On a Macintosh using HomeBrew:

```shell
brew install influxdb
```

Make sure you are connected to your Kubernetes cluster and run the following command to install InfluxDB:

```shell
helm install stable/influxdb --name influxdb --namespace influxdb
```

This will create a service named `influxdb.influxdb.svc`. You'll need that string below.

Port forward to access InfluxDB locally:

```shell
kubectl port-forward --namespace influxdb $(kubectl get pods --namespace influxdb -l app=influxdb -o jsonpath='{ .items[0].metadata.name }') 8086:8086
```

Connect to influxDB, using the `influx` client command you just installed on your workstation, and create one or both of the following databases, depending on which of the two apps you intend to run:

```shell
influx -execute 'create database airline_ml' -host localhost -port 8086
influx -execute 'create database wine_ml' -host localhost -port 8086
```

You can use different database names, but make the corresponding configuration change in the following steps.

If you changed anything above, the service name, the port, or the database name used, you'll need to edit one or both configuration files:

* Wine app: `wine-quality-ml/src/main/resources/reference.conf`
* Airlines app: `airline-flights-ml/src/main/resources/reference.conf`

Edit the `host`, `port`, and `database` field to match your setup. Here is the default content for the Wine app:

```
...
influxdb : {
  host : "influxdb.influxdb.svc",
  port : 8086,
  database : "wine_ml"
}
```

For the Airline app, it is the same, except for the database name, in `airline-flights-ml/src/main/resources/reference.conf`:


```
influxdb : {
  host : "influxdb.influxdb.svc",
  port : 8086,
  database : "airline_ml"
}
```

> NOTE: You can also override these config values on the command-line, as discussed below.

### Setup Kubeflow - Recommender Example

Kubeflow is used for the recommender example.

Instructions - TBD

### Air Traffic H20 Example

The `RecordsReader` class under the `util` project is able to load files from the `CLASSPATH`, a file system (POSIX, not HDFS), and from URLs. There is also an API that determines from the configuration while which source to use. This logic is used by the wine and airline apps to load their data and model files.

For the airline app, there is a truncated data file from 1990, about 1MB in size, in the `data` subproject, but the default configuration in `airline-flights-ml/src/main/resources/reference.conf/` has entries to pull down many large files from the original URL, storing them locally in the running image for the ingress object. By default, all but one are commented out. You may wish to add a few more, but _if this pod runs out of memory, remove some of them from the list!_

> WARNING: If you decide to load files from the `CLASSPATH` instead, keep in mind that these files are bundled into the application Docker image, so avoid loading too many of them or the image size will be huge!

In contrast the airline app does not attempt to load new model files. The single model is stored in `data/src/main/resources/airline/models` and loaded from the `CLASSPATH` at startup.

## Build and Deploy the Applications

Decide which of the three projects you want to build and deploy, then change to that project in `sbt` and run `buildAndPublish`. If you run any of the following commands in the "root" project (`pipelines-model-serving`), you'll get errors about multiple blueprint files being disallowed.

So, from the `sbt` prompt, do _one_ of the following first:

1. Wine quality: `project wineModelServingPipeline` (corresponding to the directory `wine-quality-ml`)
2. Airline flights: `project airlineFlightsModelServingPipeline` (corresponding to the directory `airline-flights-ml`)
3. Recommender: `project recommenderModelServingPipeline` (corresponding to the directory `recommender-ml`)

Now build. First, you can explicitly verify the blueprint, although this command is also run as part of `buildAndPublish` next:

```
sbt verifyBlueprint
```

Build the project:

```
sbt buildAndPublish
```

The image name will be based on one of the following strings, where `USER` will be replaced with your user name at build time:

* Wine app: `wine-quality-ml-USER`
* Airline app: `airline-flights-ml-USER`
* Recommender app: `recommender-ml-USER`

The full image name, including the Docker registry URL in your cluster and the auto-generated tag for the image, is printed as part of the output of the `buildAndPublish` command. Copy and past that text for the deployment command next, replacing the placeholder `IMAGE` shown:

```shell
kubectl pipelines deploy IMAGE
```

For the airline and wine apps, you can also override InfluxDB parameters on the command line (or any other parameters, really). For the wine app it would look as follows, where any or all of the configuration flags could be supplied. Here, the default values are shown on the right hand sides of the equal signs:

```shell
kubectl pipelines deploy IMAGE \
  wine-quality.influxdb.host="influxdb.influxdb.svc" \
  wine-quality.influxdb.port="8086" \
  wine-quality.influxdb.database="wine_ml"
```

Similarly, for the airline apps:

```shell
kubectl pipelines deploy IMAGE \
  airline-flights.influxdb.host="influxdb.influxdb.svc" \
  airline-flights.influxdb.port="8086" \
  airline-flights.influxdb.database="airline_ml"
```

## Notes

There are a number of `main` routines available for "mini-testing". If you just type `run` in one of the top-level projects mentioned above, you'll invoke a Pipelines runner under development, which may not be what you want. Instead, use the `sbt show determineMainClasses` to find the full paths and then use `sbt runMain foo.bar.Baz` to run the specific example you want.

The wine data is inspired by this data source:
https://www.kaggle.com/uciml/red-wine-quality-cortez-et-al-2009

The airline data comes from this data set, where you can see the full list of available data files. The airline app pulls some of these files directly from this location at startup:
http://stat-computing.org/dataexpo/2009/the-data.html

Copyright (C) 2019 Lightbend Inc. (https://www.lightbend.com).

