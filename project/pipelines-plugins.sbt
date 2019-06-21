// Resolver for the pipelines-sbt plugin
//
// NOTE: Private repository!
//  Please add your Bintray credentials to your global SBT config.
//
// Refer to https://developer.lightbend.com/docs/pipelines/current/#_installing
// for details on how to setup your Bintray credentials, which is required to access `sbt-pipelines`.
//

resolvers += Resolver.url("Pipelines Internal", url("https://dl.bintray.com/lightbend/pipelines-internal"))(Resolver.ivyStylePatterns)
resolvers += Resolver.url("lightbend-commercial", url("https://repo.lightbend.com/commercial-releases"))(Resolver.ivyStylePatterns)
resolvers += "Akka Snapshots" at "https://repo.akka.io/snapshots/"

addSbtPlugin("com.lightbend.pipelines" % "sbt-pipelines" % "1.0.1-985-813d5687")
// addSbtPlugin("com.lightbend.pipelines" % "sbt-pipelines" % "1.0.1-966-599912c2")
// addSbtPlugin("com.lightbend.pipelines" % "sbt-pipelines" % "1.0.1-957-fa92a61c")
// addSbtPlugin("com.lightbend.pipelines" % "sbt-pipelines" % "1.0.0")
