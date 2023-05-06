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
//  "commons-beanutils" % "commons-beanutils" % "1.9.4",
//  "org.apache.commons" % "commons-text" % "1.10.0",
  "com.lihaoyi" %% "upickle" % "3.1.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1"
)