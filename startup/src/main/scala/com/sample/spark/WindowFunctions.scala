package com.sample.spark

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, lead, unix_timestamp}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object WindowFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("RandomForestClassifierExample")
    .getOrCreate()

  import spark.implicits._

  val someDF = Seq(
    ("2018-10-08 05:00:00","s1"),
    ("2018-10-08 05:15:00","s1"),
    ("2018-10-08 05:45:00","s1"),

    ("2018-10-08 05:45:00","s2"),
    ("2018-10-08 06:15:00","s2"),
    ("2018-10-08 06:45:00","s2"),

    ("2018-10-08 05:00:00","s3"),
    ("2018-10-08 05:15:00","s3"),
    ("2018-10-08 05:45:00","s3"),

    ("2018-10-08 05:45:00","s4"),
    ("2018-10-08 06:15:00","s4"),
    ("2018-10-08 06:45:00","s4")
  ).toDF("utc_starttime","siteid")

  someDF.show(20,false)

  val nextStartTimeIntervalSpec = Window.orderBy(col("utc_starttime"))

  val distinctSiteIdList = someDF.select(someDF("siteid")).distinct.rdd.map(r => r(0)).collect().toList

  println("distinctSiteIdList "+distinctSiteIdList)

  def findMissingInterval(rollupInMin: Int, nextStartTimeIntervalSpec: WindowSpec): Dataset[Row] = {
    val one = someDF.groupBy(col("utc_starttime"),col("siteid")).agg(col("utc_starttime").as("unique_utc_starttime")).distinct()
    println("==== ONE ====")
    one.show(20,false)
    val two = one.withColumn("next_interval", lead(col("unique_utc_starttime"), 1).over(nextStartTimeIntervalSpec))
    println("==== TWO ====")
    two.show(20,false)
    val three = two.filter((unix_timestamp(col("next_interval")) - unix_timestamp(col("unique_utc_starttime"))) > (rollupInMin * 60))
    println("==== THREE ====")
    three.show(20,false)
    three
  }


  findMissingInterval(15, nextStartTimeIntervalSpec).show(20)

}
