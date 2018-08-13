package com.sample.spark

import FakeFriends.rddTuple
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FakeFriends extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "FakeFriends")
  //File input: 0,Will,33,385 -index,name,age,#friends
  val rdd: RDD[String] = sc.textFile("/Users/donthgo/sparkscala/SparkScalaDownloaded/fakefriends.csv")
  val rddTuple: RDD[(Int, Int)] = rdd.map(getTuple)

  myWay(rddTuple)
  println("=======================")
  lectureWay(rddTuple)


  def myWay(rddTuple: RDD[(Int, Int)]) = {
    val ageMap: RDD[(Int, Iterable[Int])] = rddTuple.groupByKey()
    val result: RDD[(Int, Int)] = ageMap.map(each => (each._1, each._2.sum / each._2.size))
    result.sortByKey().collect().map(println)
  }

  def lectureWay(rddTuple: RDD[(Int, Int)]) = {
    val grouped: RDD[(Int, (Int, Int))] = rddTuple.mapValues(v => (v, 1)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    grouped.mapValues(v => v._1 / v._2).sortByKey().collect().map(println)
  }


  //Returns (Int,Int) => (age,#friends)
  def getTuple(line: String): (Int, Int) = {
    val array = line.split(",")
    (array(2).toInt, array(3).toInt)
  }
}
