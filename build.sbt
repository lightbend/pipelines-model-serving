import sbt._
import sbt.Keys._
import scalariform.formatter.preferences._

lazy val root = modelServingPipeline

val tensorflowVersion     = "1.12.0"
val PMMLVersion           = "1.4.3"

version := "1.0"

lazy val modelServingPipeline = (project in file("./model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .settings(
    name := "ml-serving-pipeline",
    version := "1.0",
    mainBlueprint := Some("blueprint.conf"),
    pipelinesDockerRegistry := Some("docker-registry-default.gsa2.lightbend.com"),
    libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
  )
  .dependsOn(wineDataIngestor,modelServingFlow, modelServingEgress)

lazy val datamodel = (project in file("./datamodel"))
  .enablePlugins(PipelinesLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.twitter"   %% "bijection-avro" % "0.9.6",
      "org.scalatest" %% "scalatest"      % "3.0.5"    % "test",
      "tech.allegro.schema.json2avro" % "converter" % "0.2.8"
    ),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val wineDataIngestor= (project in file("./wine-data-ingestor"))
    .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
    .settings(
      commonSettings,
      libraryDependencies ++= Seq(
        "com.typesafe.akka"         %% "akka-http-spray-json"   % "10.1.6",
        "org.scalatest"             %% "scalatest"              % "3.0.5"    % "test",
        "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.0-RC1"
      )
    )
  .dependsOn(datamodel)

lazy val modelServingFlow= (project in file("./model-serving-flow"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka"         %% "akka-http-spray-json"   % "10.1.6",
      "org.scalatest"             %% "scalatest"              % "3.0.5"    % "test",
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.0-RC1",
      "org.tensorflow"          % "tensorflow"                          % tensorflowVersion,
      "org.tensorflow"          % "proto"                               % tensorflowVersion,
      "org.jpmml"               % "pmml-evaluator"                      % PMMLVersion,
      "org.jpmml"               % "pmml-evaluator-extension"            % PMMLVersion
    )
  )
  .dependsOn(datamodel)

lazy val modelServingEgress = (project in file("./model-serving-egress"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka"         %% "akka-http-spray-json"   % "10.1.6",
      "org.scalatest"             %% "scalatest"              % "3.0.5"    % "test",
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.0-RC1",
      "org.influxdb"  % "influxdb-java" % "2.15"
    )
  )
  .dependsOn(datamodel)

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
