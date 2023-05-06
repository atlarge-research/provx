ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "provxlib",
    idePackagePrefix := Some("lu.magalhaes.gilles.provxlib")
  )

val sparkVersion = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)