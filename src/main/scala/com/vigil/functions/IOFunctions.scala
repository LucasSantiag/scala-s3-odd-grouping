package com.vigil.functions

import com.vigil.KeyPair
import com.vigil.functions.RDDHelpersFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object IOFunctions {
  private lazy val TSV_EXTENSION = "tsv"

  private val dataSchema: StructType = StructType(Array(
    StructField("key", StringType, nullable = true),
    StructField("value", IntegerType, nullable = true),
  ))

  /**
   * Provides the specific delimiter for each file type used in the application
   *
   * @param fileType indicates if is a TSV or a CSV
   * @return String delimiter
   */
  private def getDelimiterByType(fileType: String) = if (fileType == TSV_EXTENSION) "\t" else ","

  /**
   * Read all files from path based on file type, will ignore custom headers at each file and convert to
   * key|value columns format
   *
   * @param spark SparkSession
   * @param fileType String indicating file type
   * @param inputPath S3 path to read
   * @return
   */
  def readFileByType(spark: SparkSession, fileType: String, inputPath: String): DataFrame = {
    spark.read.format("csv")
      .option("delimiter", getDelimiterByType(fileType))
      .option("header", "true")
      .schema(dataSchema)
      .load(s"$inputPath/*.${fileType}")
  }

  /**
   * Write the DataFrame as a TSV File
   *
   * @param df DataFrame to write
   * @param outputPath Path to write
   */
  def writeTsv(df: DataFrame, outputPath: String): Unit =
    df.write.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .save(s"$outputPath")

  /**
   * Read input path as an RDD, ignoring all random headers and converting any empty value in zero.
   *
   * @param sc SparkContext
   * @param inputPath input path to read file
   * @return RDD with formatted content
   */
  def readRDD(sc: SparkContext, inputPath: String): RDD[KeyPair] = {
    sc
      .textFile(s"$inputPath/*")
      .map(splitByDelimiters)
      .flatMap(removeRandomStringHeaders)
      .map(transformEmptyLineInZero)
  }

  /**
   * Write a RDD[(Int, Int)] in a TSV formatted file
   *
   * @param rdd RDD[KeyPair] (Int, Int)
   * @param outputPath output path to write
   */
  def writeTsvFromRDD(rdd: RDD[KeyPair], outputPath: String): Unit =
    rdd
      .map(toTsvFormat)
      .saveAsTextFile(s"$outputPath")
}
