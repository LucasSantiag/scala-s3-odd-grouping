package com.vigil.functions

import com.vigil.utils.SparkSessionProviderTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

class AggregateFunctionsTest extends AnyFunSuite with SparkSessionProviderTest {
  import spark.implicits._

  test("Empty valued DataFrame should be filled with zeros") {
    val emptyValuedDataFrame = Seq(1, 2, 3).toDF().withColumn("value", lit(null).cast(IntegerType))
    val resultDF = AggregateFunctions.fillEmptyValues(emptyValuedDataFrame)
    assert(resultDF.collect().forall(!_.anyNull))
  }

  test("count occurrences by row based on key, value columns") {
    val df = Seq((1, 2), (1, 2), (1, 2), (2, 2)).toDF("key", "value")
    val resultDF = AggregateFunctions.occurrencesByKey(df)
    assert(resultDF.count() == 2)
    assert(resultDF.collect().head == Row(1, 2, 3))
    assert(resultDF.collect().last == Row(2, 2, 1))
  }

  test("dataframe should be filtered only by count(value) odd numbers") {
    val df = Seq((1, 2, 1), (2, 2, 4), (3, 2, 3), (4, 2, 2)).toDF("key", "value", "count(value)")
    val resultDF = AggregateFunctions.filterByOddCount(df)
    assert(resultDF.count() == 2)
    assert(resultDF.collect().head == Row(1, 2, 1))
    assert(resultDF.collect().last == Row(3, 2, 3))
  }

  test("returns dataframe with only key and value columns") {
    val df = Seq((1, 2, 3)).toDF("key", "value", "otherColumn")
    val resultDF = AggregateFunctions.getPairColumns(df)
    assert(resultDF.columns sameElements Array("key", "value"))
  }

  test("rdd with tuple of integer should return a grouped by count as third integer") {
    val rdd = spark.sparkContext.parallelize(Seq((1, 2), (1, 2), (2, 2)))
    val resultRDD = AggregateFunctions.occurrencesByKeyRDD(rdd)
    assert(resultRDD.collect().head == ((1, 2), 2))
    assert(resultRDD.collect().last == ((2, 2), 1))
  }

  test("rdd with key value pair and group by count should be filtered with only odd keypair count") {
    val rdd = spark.sparkContext.parallelize(Seq(((1, 2), 1), ((2, 2), 2), ((3, 2), 3)))
    val resultRDD = AggregateFunctions.filterByOddCountRDD(rdd)
    assert(resultRDD.collect().length == 2)
    assert(resultRDD.collect().head == ((1, 2), 1))
    assert(resultRDD.collect().last == ((3, 2), 3))
  }

  test("rdd should return only keys") {
    val rdd = spark.sparkContext.parallelize(Seq(((1, 2), 3)))
    val resultRDD = AggregateFunctions.getPairColumnsRDD(rdd)
    assert(resultRDD.collect().head == (1, 2))
  }
}
