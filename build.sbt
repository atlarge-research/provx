ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "provxlib",
    idePackagePrefix := Some("lu.magalhaes.gilles.provxlib")
  )

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "commons-configuration" % "commons-configuration" % "1.10",
  "com.lihaoyi" %% "upickle" % "3.1.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.lihaoyi" %% "mainargs" % "0.5.0",
  "com.lihaoyi" %% "sourcecode" % "0.3.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scala-graph" %% "graph-core" % "2.0.0",
  "org.scala-graph" %% "graph-dot" % "2.0.0"
)