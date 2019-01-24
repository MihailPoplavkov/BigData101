package com.epam.bigdata.poplavkov.streaming

import java.util.Properties
import java.util.concurrent._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.io.Source

object CSVProducer {
  val Capacity = 10

  val filename = "/root/train.csv"
  val queue: BlockingQueue[Iterable[String]] = new LinkedBlockingQueue[Iterable[String]](Capacity)

  def main(args: Array[String]): Unit = {
    val batchSize: Int = args(0).toInt
    val poolSize: Int = args(1).toInt
    val topic = args(2)
    val brokers = args(3)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "CSVProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new Thread {
      override def run() {
        var total = 0
        val name = Thread.currentThread().getName
        println(s"$name: Reader started")

        @tailrec
        def fillQueue(stream: Stream[String]): Unit = {
          if (stream.isEmpty) {
            Unit
          } else {
            stream.splitAt(batchSize) match {
              case (head, tail) =>
                queue.put(head)
                total += head.size
                println(s"$name: Put ${head.size} records into queue. Total: $total")
                fillQueue(tail)
            }
          }
        }

        fillQueue(Source.fromFile(filename).getLines().toStream)
        println(s"$name: Reader ended")
      }
    }.start()

    for (_ <- 1 to poolSize) {
      new Thread {
        override def run() {
          val name = Thread.currentThread().getName
          println(s"$name: Kafka producer started")
          val producer = new KafkaProducer[String, String](props)
          try {
            while (true) {
              val records = queue.poll(5, TimeUnit.SECONDS)
              records.foreach { rec =>
                val data = new ProducerRecord[String, String](topic, rec)
                producer.send(data)
              }
              producer.flush()
              println(s"$name: ${records.size} records were sent")
            }
          } catch {
            case _: Throwable => println(s"$name: Kafka producer ended")
              producer.close()
          }
        }
      }.start()
    }
  }
}
       