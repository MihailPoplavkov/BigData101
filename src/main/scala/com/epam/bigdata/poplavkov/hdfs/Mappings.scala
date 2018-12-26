package com.epam.bigdata.poplavkov.hdfs

import com.epam.bigdata.poplavkov.hdfs.ValueType.{Floating, Num, Str, ValueType}

// This file consists of column types of the given csv files
object Mappings {
  private val sampleSubmissionTypes: Seq[ValueType] = Seq(Num, Str)
  private val destinationsTypes: Seq[ValueType] = Num +: (1 to 149).map(_ => Floating)
  private val testTypes: Seq[ValueType] =
    Seq(Num, Str, Num, Num, Num, Num, Num, Floating, Num, Num, Num, Num, Str, Str, Num, Num, Num, Num, Num, Num, Num, Num)
  private val trainTypes: Seq[ValueType] =
    Seq(Str, Num, Num, Num, Num, Num, Floating, Num, Num, Num, Num, Str, Str, Num, Num, Num, Num, Num, Num, Num, Num, Num, Num)

  val filesWithTypes: Map[String, Seq[ValueType]] = Map(
    "sample_submission.csv" -> sampleSubmissionTypes,
    "destinations.csv" -> destinationsTypes,
    "test.csv" -> testTypes,
    "train.csv" -> trainTypes)

}
