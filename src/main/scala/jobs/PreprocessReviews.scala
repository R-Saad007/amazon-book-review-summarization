package jobs

// Usage: spark-submit --class jobs.PreprocessReviews your-jar.jar /input/path /output/path

import org.apache.spark.sql.functions._
import io.{DataLoader, DataWriter}
import preprocess.TextCleaner
import utils.SparkUtils

object PreprocessReviews {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: PreprocessReviews <inputPath> <outputPath>")
      println("  inputPath: Path to sampled reviews (e.g., books_sample_5k_min10)")
      println("  outputPath: Path to save preprocessed data")
      sys.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    // Create Spark session
    val spark = SparkUtils.createSparkSession("PreprocessBookReviews")
    import spark.implicits._

    println(s"Loading reviews from: $inputPath")

    // Load the sampled data
    val reviews = DataLoader.loadJson(spark, inputPath)

    // Preprocess reviews using modular components
    val preprocessed = reviews
      .withColumn("text_clean", TextCleaner.cleanTextUDF(col("reviewText")))
      .withColumn("text_length", length(col("text_clean")))
      // Filter: text must be non-empty
      .filter(col("text_clean") =!= "")
      // Filter: reasonable length constraints
      .filter(col("text_length") >= 50 && col("text_length") <= 2000)
      // Select final columns
      .select(
        col("asin"),
        col("text_clean").alias("text"),
        col("text_length")
      )

    val totalReviews = preprocessed.count()
    println(s"Preprocessed reviews: $totalReviews")

    // Calculate statistics
    val stats = preprocessed.agg(
      avg("text_length").alias("avg_text_length"),
      min("text_length").alias("min_text_length"),
      max("text_length").alias("max_text_length")
    ).first()

    // Calculate book-level statistics
    val bookStats = preprocessed
      .groupBy("asin")
      .agg(count("*").alias("review_count"))
      .agg(
        count("*").alias("total_books"),
        avg("review_count").alias("avg_reviews_per_book"),
        min("review_count").alias("min_reviews_per_book"),
        max("review_count").alias("max_reviews_per_book")
      ).first()

    println("\n=== Statistics ===")
    println(f"Total reviews: $totalReviews")
    println(f"Total books: ${bookStats.getLong(0)}")
    println(f"Average text length: ${stats.getDouble(0)}%.1f")
    println(f"Text length range: ${stats.getInt(1)} - ${stats.getInt(2)}")
    println(f"Average reviews per book: ${bookStats.getDouble(1)}%.1f")
    println(f"Reviews per book range: ${bookStats.getLong(2)} - ${bookStats.getLong(3)}")

    // Save preprocessed data
    println(s"\nSaving to: $outputPath")
    DataWriter.saveAsJson(preprocessed, outputPath)
    println(s"Saved preprocessed data to: $outputPath")

    spark.stop()
  }
}
