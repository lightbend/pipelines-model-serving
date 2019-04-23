## Setup

### InfluxDB setup
First Install the Influx DB CLI
> brew install influxdb

Make sure you are connected to the kubernetes cluster and run the command below to install InfluxDB
> helm install stable/influxdb --name influxdb --namespace influxdb

Port forward to access InfluxDB locally 
> kubectl port-forward --namespace killrweather $(kubectl get pods --namespace influxdb -l app=influxdb -o jsonpath='{ .items[0].metadata.name }') 8086:8086

Connect to influxDB and create Database
>  influx -execute 'create database OptionScalping' -host localhost -port 8086


### Build and Deploy Killrweather Pipeline

Build the Killrweather project
> sbt buildAndPublish

Deploy the Project
> pipectl application deploy killrweather-pipeline