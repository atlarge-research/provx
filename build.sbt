ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "provxlib",
    idePackagePrefix := Some("lu.magalhaes.gilles.provxlib")
  )

val sparkVersion = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "commons-configuration" % "commons-configuration" % "1.10",
  "com.lihaoyi" %% "upickle" % "3.1.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
  "com.lihaoyi" %% "requests" % "0.8.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)