package jobs

// Usage: spark-submit --class jobs.SampleBooks your-jar.jar /input/path [/output/path] [targetReviews]
// Example: spark-submit --class jobs.SampleBooks jar data/Books.jsonl books_sample_10k 10000

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import io.{DataLoader, DataWriter}
import utils.SparkUtils

object SampleBooks {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SampleBooks <inputPath> [outputPath] [targetReviews]")
      println("  inputPath: Path to input JSON file(s) (required)")
      println("  outputPath: Output directory (default: ./books_sample_10k_min10)")
      println("  targetReviews: Target number of reviews to sample (default: 10000)")
      sys.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = if (args.length > 1) args(1) else "./books_sample_10k_min10"
    val targetReviews = if (args.length > 2) args(2).toInt else 10000

    // Create Spark session
    val spark = SparkUtils.createSparkSession("AmazonBookReviewSampler")
    import spark.implicits._

    spark.conf.set("spark.sql.files.maxPartitionBytes", 64L * 1024 * 1024)

    // --------------------------------------------------------
    // 2. Load JSON (Amazon review format)
    // --------------------------------------------------------
    val raw = spark.read
      .option("multiLine", "false")
      .json(inputPath)

    val df = raw
      .withColumn("asin", col("asin"))
      .withColumn("reviewText", col("text"))
      .withColumn("summary", col("title"))
      .withColumn("overall", col("rating").cast("double"))
      .withColumn("helpful_up", coalesce(col("helpful_vote").cast("int"), lit(0)))
      .withColumn("helpful_total", coalesce(col("helpful_vote").cast("int"), lit(0)))
      .filter(col("asin").isNotNull && col("text").isNotNull && length(trim(col("text"))) > 0)

    println(s"Loaded ${df.count()} raw reviews")

    // --------------------------------------------------------
    // 3. Filter: keep only books with ≥10 reviews
    // --------------------------------------------------------
    val counts = df.groupBy("asin").agg(count("*").alias("n_reviews"))
    val booksWithEnough = counts.filter($"n_reviews" >= 10).select("asin")
    val filtered = df.join(booksWithEnough, Seq("asin"), "inner")

    println(s"After filtering: ${filtered.count()} reviews across ${booksWithEnough.count()} books (≥10 reviews each)")

    // --------------------------------------------------------
    // 4. Cap per-book reviews to 200 (avoid domination)
    // --------------------------------------------------------
    val w = Window.partitionBy("asin").orderBy(desc("helpful_up"), desc("helpful_total"))

    val capped = filtered
      .withColumn("rn", row_number().over(w))
      .where(col("rn") <= 200)
      .drop("rn")

    println(s"After capping: ${capped.count()} reviews retained")

    // --------------------------------------------------------
    // 5. Sample books (not individual reviews) to get target number of reviews
    // --------------------------------------------------------
    val seed = 42L

    // Get book-level statistics
    val bookStats = capped
      .groupBy("asin")
      .agg(count("*").alias("review_count"))
      .orderBy(rand(seed))

    // Select books until we reach approximately target number of reviews
    println(s"Target reviews to sample: $targetReviews")
    var cumulativeReviews = 0
    var selectedBooks = List[String]()

    val bookStatsCollected = bookStats.collect()
    for (row <- bookStatsCollected if cumulativeReviews < targetReviews) {
      val asin = row.getString(0)
      val reviewCount = row.getLong(1).toInt
      selectedBooks = selectedBooks :+ asin
      cumulativeReviews += reviewCount
    }

    println(s"Selected ${selectedBooks.length} books with approximately $cumulativeReviews reviews")

    // Filter to only selected books and keep ALL their reviews
    val sampleDF = capped
      .filter(col("asin").isin(selectedBooks: _*))

    println(s"Final sample size: ${sampleDF.count()} reviews from ${selectedBooks.length} books")

    // --------------------------------------------------------
    // 6. Save to JSON (compact per-line)
    // --------------------------------------------------------
    DataWriter.saveAsJson(sampleDF, outputPath)
    println(s"Saved sample to: $outputPath")

    spark.stop()
  }
}
