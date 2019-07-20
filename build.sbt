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
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(influx, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue
  )
  .dependsOn(pipelinesx, modelServing)

lazy val recommenderModelServingPipeline = (project in file("./recommender-ml"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := s"recommender-ml-$user",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue
  )
  .dependsOn(pipelinesx, modelServing)

lazy val airlineFlightsModelServingPipeline = (project in file("./airline-flights-ml"))
  .enablePlugins(PipelinesApplicationPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := s"airline-flights-ml-$user",
    version := thisVersion,
    pipelinesDockerRegistry := dockerRegistry,
    libraryDependencies ++= Seq(influx, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue
  )
  .dependsOn(pipelinesx, modelServing)

lazy val pipelinesx = (project in file("./pipelinesx"))
  .enablePlugins(PipelinesLibraryPlugin)
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "pipelinesx",
    libraryDependencies ++= logging ++ Seq(/*alpakkaKafka,*/ bijection, json2avro, influx, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val modelServing = (project in file("./model-serving"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    name := "model-serving",
    libraryDependencies ++= Seq(tensorflow, tensorflowProto, pmml, pmmlextensions, h2o, bijection, json2avro, gson, scalajHTTP),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )
  .dependsOn(pipelinesx)

lazy val commonSettings = Seq(
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8",
    "-Xlog-reflective-calls",
    "-Xlint:_",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-deprecation",
    "-feature",
    "-language:_",
    "-unchecked",
    "-Xfatal-warnings",
    "-Ywarn-dead-code",                  // Warn when dead code is identified.
    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
    "-Ywarn-numeric-widen",              // Warn when numerics are widened.
    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates",            // Warn if a private member is unused.
    "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
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
