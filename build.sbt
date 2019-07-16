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

val user = sys.props.getOrElse("user.name", "unknown-user")

lazy val wineModelServingPipeline = (project in file("./wine-quality-ml"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := s"wine-quality-ml-$user",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry
  )
  .dependsOn(util, data, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val recommenderModelServingPipeline = (project in file("./recommender-ml"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := s"recommender-ml-$user",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry
  )
  .dependsOn(util, data, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val airlineFlightsModelServingPipeline = (project in file("./airline-flights-ml"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := s"airline-flights-ml-$user",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry
  )
  .dependsOn(util, data, dataModel, modelLibrary, dataIngestors, modelServingFlow, modelServingEgress)

lazy val util = (project in file("./util"))
  .enablePlugins(PipelinesLibraryPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "util",
    libraryDependencies ++= logging ++ Seq(alpakkaKafka, bijection, json2avro, influx, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

// Only contains data sets.
lazy val data = (project in file("./data"))
  .settings(
    name := "data"
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
  .dependsOn(util, data, dataModel)

lazy val dataIngestors = (project in file("./data-ingestors"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "data-ingestors",
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, compress, alpakkaFile, alpakkaKafka, scalaTest),
  )
  .dependsOn(util, data, dataModel, modelLibrary)

lazy val modelServingFlow = (project in file("./model-serving-flow"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-serving-flow",
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, alpakkaFile, alpakkaKafka, h2o, scalaTest)
  )
  .dependsOn(util, data, dataModel, modelLibrary)

lazy val modelServingEgress = (project in file("./model-serving-egress"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-serving-egress",
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, alpakkaFile, alpakkaKafka, influx, scalaTest)
  )
  .dependsOn(util, data, dataModel, modelLibrary)

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
