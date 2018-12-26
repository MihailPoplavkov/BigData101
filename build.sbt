import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "BigData101",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.1",
    libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"


  )
