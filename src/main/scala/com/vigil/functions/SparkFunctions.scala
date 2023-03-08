package com.vigil.functions

import com.vigil.config.Config
import org.apache.spark.sql.SparkSession

object SparkFunctions {
  /**
   * Create Spark Session with AWS Profile configuration
   *
   * @param config configuration class with AWS Profile
   * @return SparkSession
   */
  def createSparkSession(config: Config): SparkSession =
    SparkSession.builder()
      .appName("VigilGeneralDataEngineerApp")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", config.awsProfile)
      .getOrCreate()
}
