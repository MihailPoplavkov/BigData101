package com.epam.bigdata.poplavkov.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamingSaver {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkStreamingSaver")
    val ssc = new StreamingContext(conf, Seconds(5))

    val start = System.currentTimeMillis()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq("kstreams"), kafkaParams)
    )

    val rows = stream.map(_.value)

    rows.foreachRDD { rdd =>
      if (rdd.count() == 0) ssc.stop()
    }

    rows.saveAsTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/data/spark/streaming")

    ssc.start()
    ssc.awaitTermination()

    val end = System.currentTimeMillis()
    println(s"Elapsed time = ${end - start}")

  }

}
