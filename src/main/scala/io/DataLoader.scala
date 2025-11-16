package io

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  /**
   * Load JSON data from the specified path
   * @param spark SparkSession instance
   * @param path Path to the JSON file(s)
   * @return DataFrame containing the loaded data
   */
  def loadJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }

  /**
   * Load stop words from a text file
   * @param filePath Path to the stop words file (one word per line)
   * @return Set of stop words in lowercase
   */
  def loadStopWords(filePath: String): Set[String] = {
    try {
      scala.io.Source.fromFile(filePath)
        .getLines()
        .map(_.trim.toLowerCase)
        .filter(_.nonEmpty)
        .toSet
    } catch {
      case e: Exception =>
        println(s"Warning: Could not load stop words from $filePath: ${e.getMessage}")
        println("Using default empty stop word set")
        Set.empty[String]
    }
  }
}
