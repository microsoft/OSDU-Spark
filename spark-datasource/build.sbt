scalaVersion := "2.12.15" 

name := "OSDU Spark Connector"
// needs to match OSS Sonatype profile name
organization := "com.microsoft.spark"
version := "1.0.1"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "io.github.nur858" % "com-microsoft-osdu-api" % "0.0.4",
  "org.asynchttpclient" % "async-http-client" % "2.12.3",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.3"
)

resolvers += Resolver.mavenLocal

// coverageEnabled in ThisBuild := true

publishTo := sonatypePublishToBundle.value

ThisBuild / sonatypeCredentialHost := "oss.sonatype.org"
ThisBuild / versionScheme := Some("semver-spec")