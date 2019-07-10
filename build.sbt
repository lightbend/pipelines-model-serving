import sbt._
import sbt.Keys._
import scalariform.formatter.preferences._
import Dependencies._

lazy val thisVersion = "1.2.0"
version := thisVersion

// The following assumes an environment variable that defines the OpenShift cluster
// domain name and uses the default registry prefix. Adapt for your environment or
// simply use this (The "Some" is required):
// lazy val dockerRegistry = Some("registry-on-my.server.name")
lazy val dockerRegistry =
  sys.env.get("OPENSHIFT_CLUSTER_DOMAIN").map(
    server => s"docker-registry-default.$server")

lazy val wineModelServingPipeline = (project in file("./wine-model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "wine-model-serving-pipeline",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(slf4j, alpakkaKafka)
  )
  .dependsOn(util, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val recommenderModelServingPipeline = (project in file("./recommender-model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "recommender-model-serving-pipeline",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(slf4j, alpakkaKafka)
  )
  .dependsOn(util, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val airlineFlightsModelServingPipeline = (project in file("./airline-flights-model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "airline-flights-serving-pipeline",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(slf4j, alpakkaKafka)
  )
  .dependsOn(util, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val util = (project in file("./util"))
  .enablePlugins(PipelinesLibraryPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "util",
    libraryDependencies ++= Seq(alpakkaKafka, slf4j, bijection, json2avro, influx, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val dataModel = (project in file("./data-model"))
  .enablePlugins(PipelinesLibraryPlugin)
  .settings(
    name := "data-model",
    libraryDependencies ++= Seq(bijection,json2avro, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val modelLibrary = (project in file("./model-library"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-library",
    libraryDependencies ++= Seq(tensorflow, tensorflowProto,pmml,pmmlextensions, bijection,json2avro, gson, scalajHTTP),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )
  .dependsOn(util, dataModel)

lazy val dataIngestors = (project in file("./data-ingestors"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "data-ingestors",
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, compress, alpakkaFile, alpakkaKafka, scalaTest),
  )
  .dependsOn(util, dataModel, modelLibrary)

lazy val modelServingFlow = (project in file("./model-serving-flow"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-serving-flow",
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, alpakkaFile, alpakkaKafka, h2o, scalaTest)
  )
  .dependsOn(util, dataModel, modelLibrary)

lazy val modelServingEgress = (project in file("./model-serving-egress"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-serving-egress",
    commonSettings,
    libraryDependencies ++= Seq(slf4j, akkaSprayJson, alpakkaFile, alpakkaKafka, influx, scalaTest)
  )
  .dependsOn(util, dataModel, modelLibrary)

// For testing outside Pipelines, when we need to wire a few components together
// that don't have explicit dependencies above.
lazy val main = (project in file("./main"))
  .settings(
    name := "main",
    version := thisVersion,
    libraryDependencies ++= Seq(slf4j, alpakkaKafka)
  )
  .dependsOn(util, model, datamodel, dataIngestors, modelServingFlow, modelServingEgress)

lazy val commonSettings = Seq(
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8",
    "-Xlog-reflective-calls",
    "-Xlint",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-deprecation",
    "-feature",
    "-language:_",
    "-unchecked"
  ),

  scalacOptions in (Compile, console) --= Seq("-Ywarn-unused", "-Ywarn-unused-import"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,

  scalariformPreferences := scalariformPreferences.value
    .setPreference(AlignParameters, false)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 90)
    .setPreference(DoubleIndentConstructorArguments, true)
    .setPreference(DoubleIndentMethodDeclaration, true)
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(NewlineAtEndOfFile, true)
    .setPreference(AllowParamGroupsOnNewlines, true)
)
