package com.vigil

import com.vigil.config.{Config, ConfigParser}
import com.vigil.functions.AggregateFunctions._
import com.vigil.functions.IOFunctions._
import com.vigil.functions.SparkFunctions._
import scopt.OParser

object VigilGeneralDataEngineerApp {
  /**
   * The main method to start processing all csv+tsv files
   *
   * Receives as input parameters:
   *  -i, --input <value>      Input path
   *  -o, --output <value>     Outut path
   *  -a, --aws-profile <value> Aws profile
   */
  def main(args: Array[String]): Unit = {
    OParser.parse(ConfigParser.argParser, args, Config()) match {
      case Some(config) =>
        executeWithRDDAPI(config)
      case None =>
        throw new IllegalArgumentException("Missing required arguments.")
    }
  }

  /**
   * Provides the execution of Data processing flow utilizing Spark DataFrame API
   *
   * @param config Program variables configuration (input, output and awsProfile)
   */
  private def executeWithDataFrameAPI(config: Config): Unit = {
    val spark = createSparkSession(config)

    val dataframe = {
      val csvDataFrame = readFileByType(spark, "csv", config.input)
      val tsvDataFrame = readFileByType(spark, "tsv", config.input)
      csvDataFrame.union(tsvDataFrame)
    }

    val filledDataFrame = fillEmptyValues(dataframe)
    val groupedByKeyDataFrame = occurrencesByKey(filledDataFrame)
    val filterByOddCountDataFrame = filterByOddCount(groupedByKeyDataFrame)
    val finalDataFrame = getPairColumns(filterByOddCountDataFrame)

    writeTsv(finalDataFrame, config.output)
  }

  /**
   * provides the execution of Data processing flow utilizing Spark RDD API
   *
   * @param config Program variables configuration (input, output and awsProfile)
   * */
  private def executeWithRDDAPI(config: Config): Unit = {
    val sc = createSparkSession(config).sparkContext

    val rdd = readRDD(sc, config.input)

    val groupedByKeyRDD = occurrencesByKeyRDD(rdd)
    val filterByOddCount = filterByOddCountRDD(groupedByKeyRDD)
    val finalRDD = getPairColumnsRDD(filterByOddCount)

    writeTsvFromRDD(finalRDD, config.output)
  }
}
