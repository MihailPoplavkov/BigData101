package com.epam.bigdata.poplavkov.streaming

import org.apache.spark.sql.SparkSession

object SparkSaver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Task 1").getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis()

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "kstreams")
      .load()

    df.selectExpr("CAST(value AS STRING)")
      .as[String]
      .write
      .option("header", "true")
      .csv("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/data/batch")

    val end = System.currentTimeMillis()
    println(s"Elapsed time = ${end - start}")

    spark.stop()

  }

}
