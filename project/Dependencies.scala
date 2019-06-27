import Versions._
import sbt._

object Dependencies {

  val bijection       = "com.twitter"                    %% "bijection-avro"            % bijectionVersion
  val json2avro       = "tech.allegro.schema.json2avro"   % "converter"                 % json2javaVersion
  val akkaSprayJson   = "com.typesafe.akka"              %% "akka-http-spray-json"      % akkaHTTPJSONVersion
  val alpakkaFile     = "com.lightbend.akka"             %% "akka-stream-alpakka-file"  % alpakkaFileVersion
  val alpakkaKafka    = "com.typesafe.akka"              %% "akka-stream-kafka"         % alpakkaKafkaVersion
  val tensorflow      = "org.tensorflow"                  % "tensorflow"                % tensorflowVersion
  val tensorflowProto = "org.tensorflow"                  % "proto"                     % tensorflowVersion
  val pmml            = "org.jpmml"                       % "pmml-evaluator"            % PMMLVersion
  val pmmlextensions  = "org.jpmml"                       % "pmml-evaluator-extension"  % PMMLVersion
  val influx          = "org.influxdb"                    % "influxdb-java"             % influxVerion
  val slf4j           = "org.slf4j"                       % "slf4j-log4j12"             % slf4jVersion
  val gson            = "com.google.code.gson"            % "gson"                      % gsonVersion
  val scalajHTTP      = "org.scalaj"                     %% "scalaj-http"               % scalajHTTPVersion
  // val scalaLogging    = "com.typesafe.scala-logging"     %% "scala-logging"             % scalaLoggingVersion
  // val logback         = "ch.qos.logback"                  % "logback-classic"           % logbackClassicVersion

  val scalaTest       = "org.scalatest"                  %% "scalatest"                 % scaltestVersion    % "test"
  // Only used in tests:
  val SprayJson       = "io.spray"                       %% "spray-json"                % sprayJsonVersion   % "test"
}
