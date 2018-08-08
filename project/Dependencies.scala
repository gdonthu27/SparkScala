import sbt._

object Dependencies {
  val scalaTestVersion = "3.0.1"
  val logbackVersion = "1.1.5"
  val sparkVersion = "2.3.0"

  val ParkingSpark = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion  exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.spark" %% "spark-streaming" % sparkVersion ,
    "org.apache.spark" %% "spark-sql" % sparkVersion ,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "org.scalatest" %% "scalatest" % "2.2.4" % Test
  )

}
