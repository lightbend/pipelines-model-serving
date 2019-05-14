import sbt._
import sbt.Keys._
import scalariform.formatter.preferences._
import Dependencies._

lazy val root = modelServingPipeline

version := "1.0"

lazy val modelServingPipeline = (project in file("./model-serving-pipeline"))
  .enablePlugins(PipelinesApplicationPlugin)
  .settings(
    name := "ml-serving-pipeline",
    version := "1.0",
    mainBlueprint := Some("blueprint.conf"),
    pipelinesDockerRegistry := Some("docker-registry-default.gsa2.lightbend.com"),
    libraryDependencies ++= Seq(slf4j, alpakkaKafka)
  )
  .dependsOn(DataIngestors,modelServingFlow, modelServingEgress)

lazy val datamodel = (project in file("./datamodel"))
  .enablePlugins(PipelinesLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(bijection,json2avro, scalaTest),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val model = (project in file("./modellibrary"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(tensorflow, tensorflowProto,pmml,pmmlextensions, bijection,json2avro, gson),
    (sourceGenerators in Compile) += (avroScalaGenerateSpecific in Compile).taskValue,
  )

lazy val DataIngestors = (project in file("./data-ingestors"))
    .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
    .settings(
      commonSettings,
      libraryDependencies ++= Seq(akkaSprayJson,alpakkaFile,scalaTest, alpakkaKafka),
    )
  .dependsOn(datamodel, model)

lazy val modelServingFlow= (project in file("./model-serving-flow"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson,alpakkaFile,scalaTest, alpakkaKafka)
  )
  .dependsOn(model, datamodel)

lazy val modelServingEgress = (project in file("./model-serving-egress"))
  .enablePlugins(PipelinesAkkaStreamsLibraryPlugin)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(akkaSprayJson,alpakkaFile,influx, scalaTest, alpakkaKafka)
  )
  .dependsOn(datamodel, model)

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