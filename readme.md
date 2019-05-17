## Setup

### InfluxDB setup
First Install the Influx DB CLI
> brew install influxdb

Make sure you are connected to the kubernetes cluster and run the command below to install InfluxDB
> helm install stable/influxdb --name influxdb --namespace influxdb

Port forward to access InfluxDB locally 
> kubectl port-forward --namespace influxdb $(kubectl get pods --namespace influxdb -l app=influxdb -o jsonpath='{ .items[0].metadata.name }') 8086:8086

Connect to influxDB and create Database
>  influx -execute 'create database wine_ml' -host localhost -port 8086


### Build and Deploy Ml Pipeline

Verify blueprint
````
sbt verifyBlueprint
````
Build the project
````
sbt buildAndPublish
````
Get the image name and tag
````
docker images
````
Deploy the Project
````
kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/model-serving-pipeline:{tag-name}

kubectl pipelines deploy docker-registry-default.gsa2.lightbend.com/lightbend/ml-serving-pipeline:{tag-name} raw-egress.InfluxHost="influxdb.influxdb.svc" raw-egress.InfluxPort="8086" raw-egress.InfluxDatabase="wine_ml" influx-result-egress.InfluxHost="influxdb.influxdb.svc" influx-result-egress.InfluxPort="8086" influx-result-egress.InfluxDatabase="wine_ml"
````
Data is inspired by this datasource: 
https://www.kaggle.com/uciml/red-wine-quality-cortez-et-al-2009

Copyright (C) 2019 Lightbend Inc. (https://www.lightbend.com).

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.