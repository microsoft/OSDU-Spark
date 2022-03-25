scalaVersion := "2.12.15" 
// scalaVersion := "2.13.1"

name := "OSDU Spark Connector"
organization := "com.microsoft"
version := "1.0"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "io.github.nur858" % "com-microsoft-osdu-api" % "0.0.4"
)

resolvers += Resolver.mavenLocal

// coverageEnabled in ThisBuild := true