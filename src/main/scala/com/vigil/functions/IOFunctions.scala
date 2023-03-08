package com.vigil.functions

import com.vigil.KeyPair
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IOFunctions {
  private lazy val S3_PREFIX: String = "s3a://"
  private lazy val TSV_EXTENSION = "tsv"

  private val dataSchema: StructType = StructType(Array(
    StructField("key", StringType, true),
    StructField("value", IntegerType, true),
  ))

  private def getDelimiterByType(fileType: String) = if (fileType == TSV_EXTENSION) "\t" else ","

  def readFileByType(spark: SparkSession, fileType: String, inputPath: String): DataFrame = {
    spark.read.format("csv")
      .option("delimiter", getDelimiterByType(fileType))
      .option("header", "true")
      .schema(dataSchema)
      .load(s"$S3_PREFIX$inputPath/*${fileType}")
  }

  def writeTsv(df: DataFrame, outputPath: String): Unit =
    df.write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .save(s"$S3_PREFIX$outputPath")

  private def splitByDelimiters(l: String): Array[String] =
    l.split(Array(',', '\t'))

  private def transformToIntOption(l: Array[String]) =
    l.map(_.toIntOption)

  private def removeRandomStringHeaders(l: Array[Option[Int]]) =
    l.forall(_.nonEmpty)

  private def transformEmptyLineInZero(l: Array[Option[Int]]) =
    l match {
      case Array(Some(key), None) => (key, 0)
      case Array(Some(key), Some(value)) => (key, value)
    }

  def readRDD(sc: SparkContext, inputPath: String): RDD[KeyPair] = {
    sc
      .textFile(inputPath)
      .map(splitByDelimiters)
      .map(transformToIntOption)
      .filter(removeRandomStringHeaders)
      .map(transformEmptyLineInZero)
  }

  private def toTsvFormat(pair: KeyPair) =
    s"${pair._1}\t${pair._2}"

  def writeTsvFromRDD(rdd: RDD[KeyPair], outputPath: String): Unit =
    rdd
      .map(toTsvFormat)
      .saveAsTextFile(s"$outputPath.tsv")
}
