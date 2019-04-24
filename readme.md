## Setup

### InfluxDB setup
First Install the Influx DB CLI
> brew install influxdb

Make sure you are connected to the kubernetes cluster and run the command below to install InfluxDB
> helm install stable/influxdb --name influxdb --namespace influxdb

Port forward to access InfluxDB locally 
> kubectl port-forward --namespace influxDB $(kubectl get pods --namespace influxdb -l app=influxdb -o jsonpath='{ .items[0].metadata.name }') 8086:8086

Connect to influxDB and create Database
>  influx -execute 'create database OptionScalping' -host localhost -port 8086


### Build and Deploy Ml Pipeline

Build the project
> sbt buildAndPublish

Get the image name and tag
> docker images

Deploy the Project
> kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/model-serving-pipeline:{tag-name}