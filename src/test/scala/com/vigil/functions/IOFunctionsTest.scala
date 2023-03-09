package com.vigil.functions

import com.vigil.utils.SparkSessionProviderTest
import org.scalatest.funsuite.AnyFunSuite

class IOFunctionsTest extends AnyFunSuite with SparkSessionProviderTest {
  test("assert read csv with proper delimiter") {
    val df = IOFunctions.readFileByType(spark, "csv", "src/test/resources/input")
    assert(df.count() == 8)
  }

  test("assert read tsv with proper delimiter") {
    val df = IOFunctions.readFileByType(spark, "tsv", "src/test/resources/input")
    assert(df.count() == 8)
  }

  test("read rdd") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources/input")
    assert(rdd.count() == 16)
  }

  test("read rdd should split the rdd by csv delimiters") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources/rdd/csv_delimiter")
    assert(rdd.count() == 2)
  }

  test("read rdd should split the rdd by tsv delimiters") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources/rdd/tsv_delimiter")
    assert(rdd.count() == 2)
  }

  test("read rdd should add zero values instead of empty ones") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources/rdd/empty_values")
    assert(rdd.count() == 2)
    assert(rdd.collect().head == (1, 0))
    assert(rdd.collect().last == (2, 0))
  }

  test("read rdd should ignore every non numeric header") {
    val rdd = IOFunctions.readRDD(spark.sparkContext, "src/test/resources/rdd/headers")
    assert(rdd.count() == 2)
  }
}
