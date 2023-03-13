package com.vigil.functions

import com.vigil.utils.SparkSessionProviderTest
import org.scalatest.funsuite.AnyFunSuite

class RDDHelpersFunctionsTest extends AnyFunSuite with SparkSessionProviderTest {
  test("removeRandomHeaders should remove non Integers lines from the RDD") {
    val rdd = spark.sparkContext.parallelize(Seq(Array("1", "2"), Array("O", "F")))
    val result = rdd.flatMap(RDDHelpersFunctions.removeRandomStringHeaders)
    assert(result.collect().head sameElements Array(Option(1), Option(2)))
  }

  test("removeRandomHeaders should keep lines with integers on it") {
    val rdd = spark.sparkContext.parallelize(Seq(Array("1", "2"), Array("2", "")))
    val result = rdd.flatMap(RDDHelpersFunctions.removeRandomStringHeaders)
    assert(result.collect().length == 2)
    assert(result.collect().head sameElements Array(Option(1), Option(2)))
    assert(result.collect().last sameElements Array(Option(2), Option.empty))
  }

  test("adjust array to tsv format") {
    val keyPair = (1, 2)
    val result = RDDHelpersFunctions.toTsvFormat(keyPair)
    assert(result equals "1\t2")
  }

  test("csv delimiter should split line") {
    val line = "1,2"
    val result = RDDHelpersFunctions.splitByDelimiters(line)
    assert(result.length == 2)
    assert(result.head equals "1")
    assert(result.last equals "2")
  }

  test("tsv delimiter should split line") {
    val line = "1\t2"
    val result = RDDHelpersFunctions.splitByDelimiters(line)
    assert(result.length == 2)
    assert(result.head equals "1")
    assert(result.last equals "2")
  }

  test("replace empty values to zero") {
    val line = Array(Option(2))
    val result = RDDHelpersFunctions.transformEmptyLineInZero(line)
    assert(result == (2, 0))
  }
}
