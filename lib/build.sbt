ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "provxlib",
    idePackagePrefix := Some("lu.magalhaes.gilles.provxlib")
  )

// In order not to have more than one Spark Session running at the same time
Test / parallelExecution := false

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",
  // will not need these anymore for reading/writing to files
  "com.lihaoyi" %% "upickle" % "3.1.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.lihaoyi" %% "mainargs" % "0.5.0",
  "com.lihaoyi" %% "sourcecode" % "0.3.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scala-graph" %% "graph-core" % "2.0.0",
  "org.scala-graph" %% "graph-dot" % "2.0.0",
  "org.scala-graph" %% "graph-json" % "2.0.0"
)
