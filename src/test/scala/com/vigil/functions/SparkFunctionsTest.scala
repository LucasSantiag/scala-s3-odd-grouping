package com.vigil.functions

import com.vigil.config.Config
import org.scalatest.funsuite.AnyFunSuite

class SparkFunctionsTest extends AnyFunSuite {
  test("providing config should create spark session") {

    assert(SparkFunctions.createSparkSession(Config()).conf.get("spark.hadoop.fs.s3a.aws.credentials.profile") == "default")
  }
}
