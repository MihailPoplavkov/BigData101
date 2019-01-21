package com.epam.bigdata.poplavkov.spark

import com.epam.bigdata.poplavkov.spark.model.Row
import org.apache.spark.sql.SparkSession

object Task2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Task 1").getOrCreate()
    import spark.implicits._

    val hotels = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/tmp/data/train.csv")
      .as[Row]

    val best: (Int, Long) = hotels
      .filter(row => row.user_location_country == row.hotel_country)
      .map(_.user_location_country)
      .groupByKey(identity)
      .count()
      .reduce((t1, t2) =>
        if (t1._2 > t2._2) {
          t1
        } else {
          t2
        }
      )

    println(s"The most popular country is ${best._1}. It was booked from the same country ${best._2} times")

    spark.stop
  }

}
