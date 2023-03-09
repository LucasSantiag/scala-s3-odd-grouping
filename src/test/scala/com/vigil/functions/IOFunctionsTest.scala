package com.vigil.functions

import com.vigil.utils.SparkSessionProviderTest
import org.scalatest.funsuite.AnyFunSuite

class IOFunctionsTest extends AnyFunSuite with SparkSessionProviderTest {
  test("assert read csv with proper delimiter") {
    val df = IOFunctions.readFileByType(spark, "csv", "src/test/resources")
    assert(df.count() == 8)
  }

  test("assert read tsv with proper delimiter") {
    val df = IOFunctions.readFileByType(spark, "tsv", "src/test/resources")
    assert(df.count() == 8)
  }

  test("read rdd") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources")
    assert(rdd.count() == 16)
  }

  test("read rdd should split the rdd by csv and tsv delimiters") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources")
  }
}
