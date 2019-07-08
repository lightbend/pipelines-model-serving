import sbt._
import sbt.Keys._
import scalariform.formatter.preferences._
import Dependencies._

lazy val root = modelServingPipeline

version := "1.0"

// The following assumes an environment variable that defines the OpenShift cluster
// domain name and uses the default registry prefix. Adapt for your environment or
// simply use this (The "Some" is required):
// lazy val dockerRegistry = Some("registry-on-my.server.name")
lazy val dockerRegistry =
  sys.env.get("OPENSHIFT_CLUSTER_DOMAIN").map(
    server => s"docker-registry-default.$server")

lazy val modelServingPipeline = (project in file("./model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .settings(
    name := "ml-serving-pipeline",
    version := "1.0",
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(slf4j, alpakkaKafka),
    mainBlueprint := Some("airline-flights-blueprint.conf")
  )
  .dependsOn(util, dataIngestors, modelServingFlow, modelServingEgress)

lazy val util = (project in file("./util"))
  .enablePlugins(PipelinesLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(bijection,json2avro, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val datamodel = (project in file("./datamodel"))
  .enablePlugins(PipelinesLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(bijection,json2avro, scalaTest), //, scalaLogging, logback),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val model = (project in file("./modellibrary"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(tensorflow, tensorflowProto,pmml,pmmlextensions, bijection,json2avro, gson, scalajHTTP),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )
  .dependsOn(util, datamodel) //modelServingFlow, modelServingEgress)

lazy val dataIngestors = (project in file("./data-ingestors"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson,alpakkaFile,scalaTest, alpakkaKafka),
  )
  .dependsOn(util, datamodel, model)

lazy val modelServingFlow= (project in file("./model-serving-flow"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson, alpakkaFile, scalaTest, alpakkaKafka, h2o)
  )
  .dependsOn(util, model, datamodel, model)

lazy val modelServingEgress = (project in file("./model-serving-egress"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson,alpakkaFile,influx, scalaTest, alpakkaKafka)
  )
  .dependsOn(util, datamodel, model)

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
