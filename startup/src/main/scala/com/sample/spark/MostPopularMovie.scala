package com.sample.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}


object MostPopularMovie extends App {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine, named RatingsCounter
  val sparkContext: SparkContext = new SparkContext("local[*]", "RatingsCounter")

  val bc = sparkContext.broadcast(loadMovieNames()) //broadcasting to all nodes in cluster to avoid retransmitting of data

  // Load up each line of the ratings data into an RDD
  val rdd: RDD[String] = sparkContext.textFile("/Users/donthgo/sparkscala/ml-100k/u.data")

  // Convert each line to a string, split it out by tabs, and extract the third field.
  // (The file format is userID, movieID, rating, timestamp)
  val ratingsRdd: RDD[Int] = rdd.map(line => line.split("\t")(1).toInt)

  val kvRdd: RDD[(Int, Int)] = ratingsRdd.map(m => (m, 1))

  val reducedRdd = kvRdd.reduceByKey((a, b) => a + b)

  val flip = reducedRdd.map(x => (x._2, x._1))

  val sortedOutput = flip.sortByKey()

  val merged = sortedOutput.map(r => (bc.value(r._2), r._1))

  val result = merged.collect() // get last element from sorted RDD

  result.foreach(println)

  //(The file format is userID, movieID, rating, timestamp)
  def loadMovieNames() = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val lines = Source.fromFile("/Users/donthgo/sparkscala/ml-100k/u.item").getLines()
    lines.foldLeft(Map[Int, String]())((acc, v) => {
      val arr = v.split('|')
      if (arr.length > 1) acc + (arr(0).toInt -> arr(1))
      else acc
    })
  }


}
