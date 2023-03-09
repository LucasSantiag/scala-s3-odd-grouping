package com.vigil.functions

import com.vigil.KeyPair
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
   * Split the String based on CSV or TSV delimiters
   *
   * @param l line parameter to apply regex to split
   * @return Split line as an Array[String]
   */
  private def splitByDelimiters(l: String): Array[String] =
    l.split("[,\\t]")

  /**
   * Transform each element to an Option of Integer to just use numeric values
   *
   * @param l Array of elements in the RDD line
   * @return Each element wrapped in Option monad
   */
  private def transformToIntOption(l: Array[String]) =
    l.map(_.toIntOption)

  /**
   * Check all lines that are fully not Integers
   *
   * @param l Array of element in the RDD line
   * @return True if all elements are not Integers
   */
  private def removeRandomStringHeaders(l: Array[Option[Int]]) =
    l.forall(_.nonEmpty)

  /**
   * Check for Integers Tuple and apply zero to any value Null
   *
   * @param l Array of element in the RDD line
   * @return Returns key and 0 if the value is null/empty and return key and value for filled values
   */
  private def transformEmptyLineInZero(l: Array[Option[Int]]) =
    l match {
      case Array(Some(key), None) => (key, 0)
      case Array(Some(key), Some(value)) => (key, value)
    }

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
      .map(transformToIntOption)
      .filter(removeRandomStringHeaders)
      .map(transformEmptyLineInZero)
  }

  /**
   * Transform Integer Tuple in a string concatenated by \t (tsv delimiter)
   *
   * @param pair KeyPair (Int, Int)
   * @return String concatenated by delimiter
   */
  private def toTsvFormat(pair: KeyPair) =
    s"${pair._1}\t${pair._2}"

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
