package com.epam.bigdata.poplavkov.hdfs

import java.net.URI
import java.util.Scanner

import com.epam.bigdata.poplavkov.hdfs.Mappings._
import com.epam.bigdata.poplavkov.hdfs.ValueType._
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem

object CSVToParquet {
  private val CsvDelimiter = ","

  private val HDFSUri = "hdfs://sandbox-hdp.hortonworks.com:8020/"
  private val conf = new Configuration
  conf.set("fs.defaultFS", HDFSUri)
  conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
  // Set HADOOP user
  System.setProperty("HADOOP_USER_NAME", "admin")
  System.setProperty("hadoop.home.dir", "/")
  private val fs: FileSystem = FileSystem.get(URI.create(HDFSUri), conf)

  // types - column value types
  private def writeAsAvro(csvFilePath: Path, types: Seq[ValueType]): Unit = {
    val (name, ext) = csvFilePath.getName.splitAt(csvFilePath.getName.indexOf('.'))
    require(".csv" == ext)
    val outputPath = new Path(csvFilePath.getParent, s"avro/$name.avro")
    val outputStream = fs.create(outputPath)
    val inputStream: FSDataInputStream = fs.open(csvFilePath)
    val scanner = new Scanner(inputStream)

    val headers: Seq[String] = scanner.nextLine().split(CsvDelimiter).toSeq
    val schemaMap = headers.zip(types)

    val avroSchema = schemaMap.foldLeft(SchemaBuilder.record(name).fields()) {
      case (builder, (fieldName, fieldType)) =>
        val newBuilder = builder.name(fieldName).`type`.nullable()
        fieldType match {
          case Str => newBuilder.stringType().noDefault()
          case Num => newBuilder.intType().noDefault()
          case Floating => newBuilder.doubleType().noDefault()
        }
    }.endRecord()

    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(avroSchema, outputStream)

    while (scanner.hasNextLine) {
      val line = scanner.nextLine()
      val record = new GenericData.Record(avroSchema)
      line.split(CsvDelimiter).zip(schemaMap).foreach {
        case (csvValue, (fieldName, fieldType)) =>
          if (csvValue.nonEmpty) {
            val value = fieldType match {
              case Str => csvValue
              case Num => csvValue.toInt
              case Floating => csvValue.toDouble
            }
            record.put(fieldName, value)
          }
      }
      dataFileWriter.append(record)
    }

    dataFileWriter.close()
    scanner.close()

  }

  def main(args: Array[String]): Unit = {
    val csvPath = new Path("/tmp/data")

    filesWithTypes.foreach {
      case (fileName, schema) =>
        writeAsAvro(new Path(csvPath, fileName), schema)
    }
  }
}
