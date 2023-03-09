package com.vigil.functions

import com.vigil.KeyPair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count}

object AggregateFunctions {
  /**
   * Fill all empty values with Zero value
   *
   * @param df DataFrame to fill empty values
   * @return DataFrame with zeros instead of empty values
   */
  def fillEmptyValues(df: DataFrame): DataFrame =
    df.na.fill(0)
  /**
   * Group all lines by (keys and value) and count how many there are in the DataFrame
   *
   * @param df DataFrame to groupBy and count
   * @return DataFrame with additional column with count value
   */
  def occurrencesByKey(df: DataFrame): DataFrame =
    df
      .groupBy("key", "value")
      .agg(count("value"))

  /**
   * Filter all values that has even count and keep the only one with odd count
   *
   * @param df DataFrame with count aggregation column (count(value))
   * @return DataFrame with only odd counts
   */
  def filterByOddCount(df: DataFrame): DataFrame =
    df.filter(col("count(value)").%(2).=!=(0))

  /**
   * Select desired columns to output file
   *
   * @param df DataFrame
   * @return DataFrame with key and value columns
   */
  def getPairColumns(df: DataFrame): DataFrame =
    df.select("key", "value")

  /**
   * Group all lines by (keys and value) and count how many there are in the RDD
   *
   * @param rdd RDD to groupBy and count
   * @return RDD with additional column with count value
   */
  def occurrencesByKeyRDD(rdd: RDD[KeyPair]): RDD[((Int, Int), Int)] =
    rdd
      .map((_, 1))
      .reduceByKey(_ + _)

  /**
   * Filter all values that has even count and keep the only one with odd count
   *
   * @param rdd with count aggregation column integer
   * @return rdd with only odd counts
   */
  def filterByOddCountRDD(rdd: RDD[(KeyPair, Int)]): RDD[((Int, Int), Int)] =
    rdd.filter(line => line._2 % 2 != 0)

  /**
   * Select desired fields to output file
   *
   * @param rdd RDD
   * @return RDD with only key and value fields
   */
  def getPairColumnsRDD(rdd: RDD[(KeyPair, Int)]): RDD[KeyPair] =
    rdd.keys
}
