package com.sample.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AddAmount extends App {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "MinTemperatures")

  // Read each line of input data
  val lines = sc.textFile("/Users/donthgo/sparkscala/SparkScalaDownloaded/customer-orders.csv")

  val parsedLines = lines.map(parseLine)

  val reducedRdd = parsedLines.reduceByKey((a, b) => a + b)

  val flip = reducedRdd.map(kv => (kv._2, kv._1))

  val output = flip.sortByKey().collect()

  output.foreach(x => println(s"id: ${x._2} amount: ${x._1}"))

}
