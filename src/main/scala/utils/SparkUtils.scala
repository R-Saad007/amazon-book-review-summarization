package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  /**
   * Create a SparkSession with common configuration
   * @param appName Name of the Spark application
   * @param master Spark master URL (default: "local[*]")
   * @param logLevel Log level for Spark (default: "WARN")
   * @return Configured SparkSession
   */
  def createSparkSession(
    appName: String,
    master: String = "local[*]",
    logLevel: String = "WARN"
  ): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  /**
   * Create a SparkSession for production (no master set, relies on spark-submit)
   * @param appName Name of the Spark application
   * @param logLevel Log level for Spark (default: "WARN")
   * @return Configured SparkSession
   */
  def createProductionSparkSession(
    appName: String,
    logLevel: String = "WARN"
  ): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)
    spark
  }
}
