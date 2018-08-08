name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.6"

import sbt.Keys._

lazy val commonSettings = Seq(
  version := "3.0.5",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Dependencies.ParkingSpark

)


lazy val startup = project.settings(commonSettings)