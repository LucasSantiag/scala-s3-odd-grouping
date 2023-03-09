package com.vigil.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionProviderTest {
  val spark: SparkSession = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()
}
