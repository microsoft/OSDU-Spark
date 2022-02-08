scalaVersion := "2.12.2"

name := "OSDU Spark Connector"
organization := "com.microsoft"
version := "1.0"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "com.microsoft" % "osdu-client" % "1.0.0" 
)

resolvers += Resolver.mavenLocal