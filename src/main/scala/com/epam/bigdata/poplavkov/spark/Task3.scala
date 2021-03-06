package com.epam.bigdata.poplavkov.spark

import com.epam.bigdata.poplavkov.spark.model.Row
import org.apache.spark.sql.SparkSession

object Task3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Task 1").getOrCreate()
    import spark.implicits._

    val hotels = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/tmp/data/train.csv")
      .as[Row]

    hotels
      .filter(_.srch_children_cnt match {
        case Some(cnt) =>
          cnt > 0
        case None =>
          false
      })
      .filter(_.is_booking == 0)
      .map(row => (row.hotel_continent, row.hotel_country, row.hotel_market))
      .groupByKey(identity)
      .count()
      .orderBy($"count(1)".desc)
      .take(3)
      .foreach {
        case ((continent, country, market), count) =>
          println(s"Continent = $continent, country = $country, market = $market, count = $count")
      }

    spark.stop
  }
}
