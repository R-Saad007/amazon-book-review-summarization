package io

import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWriter {

  /**
   * Save DataFrame as JSON with a single output file
   * @param df DataFrame to save
   * @param path Output path
   * @param mode SaveMode (default: overwrite)
   */
  def saveAsJson(df: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.coalesce(1)
      .write
      .mode(mode)
      .json(path)
  }

  /**
   * Save DataFrame as Parquet (more efficient for large datasets)
   * @param df DataFrame to save
   * @param path Output path
   * @param mode SaveMode (default: overwrite)
   */
  def saveAsParquet(df: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.coalesce(1)
      .write
      .mode(mode)
      .parquet(path)
  }
}
