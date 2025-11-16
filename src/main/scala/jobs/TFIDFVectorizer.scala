package jobs

// Compute TF-IDF from scratch without using MLlib
// Each sentence is treated as a document
// Usage: spark-submit --class jobs.TFIDFVectorizer your-jar.jar /input/path /output/path

import org.apache.spark.sql.functions._
import io.{DataLoader, DataWriter}
import features.{Tokenizer, TFIDFCalculator}
import preprocess.SentenceSplitter
import utils.SparkUtils

object TFIDFVectorizer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: TFIDFVectorizer <inputPath> <outputPath> [stopWordsPath]")
      println("  inputPath: Path to preprocessed reviews (e.g., data/preprocessed)")
      println("  outputPath: Path to save TF-IDF vectors")
      println("  stopWordsPath: Path to stop words file (optional, default: data/stopwords.txt)")
      sys.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val stopWordsPath = if (args.length > 2) args(2) else "data/stopwords.txt"

    // Create Spark session
    val spark = SparkUtils.createSparkSession("TFIDFVectorizer")
    import spark.implicits._

    // Load stop words
    println(s"Loading stop words from: $stopWordsPath")
    val stopWords = DataLoader.loadStopWords(stopWordsPath)
    println(s"Loaded ${stopWords.size} stop words")

    println(s"Loading preprocessed reviews from: $inputPath")

    // Load preprocessed data
    val reviews = DataLoader.loadJson(spark, inputPath)

    val totalReviews = reviews.count()
    println(s"Loaded $totalReviews reviews")

    // --------------------------------------------------------
    // 1. Split reviews into sentences
    // --------------------------------------------------------
    println("Splitting reviews into sentences...")

    val sentences = reviews
      .withColumn("review_id", monotonically_increasing_id())
      .withColumn("sentences", SentenceSplitter.splitSentencesUDF(col("text")))
      .withColumn("sentence", explode(col("sentences")))
      .withColumn("sentence_id", monotonically_increasing_id())
      .select("sentence_id", "review_id", "asin", "text", "sentence")

    val totalSentences = sentences.count()
    println(s"Split into $totalSentences sentences (documents)")

    // --------------------------------------------------------
    // 2. Tokenization and Term Frequency (TF) - at SENTENCE level
    // --------------------------------------------------------
    println("Computing tokenization and term frequencies for each sentence...")

    // Broadcast stop words for efficient access in UDF
    val broadcastStopWords = spark.sparkContext.broadcast(stopWords)
    val tokenizeUDF = Tokenizer.tokenizeUDF(broadcastStopWords.value)

    val sentencesWithTokens = sentences
      .withColumn("tokens", tokenizeUDF(col("sentence")))
      .withColumn("tf", TFIDFCalculator.termFrequencyUDF(col("tokens")))
      .filter(size(col("tokens")) > 0)  // Filter out empty sentences

    println(s"Tokenized ${sentencesWithTokens.count()} sentences")

    // --------------------------------------------------------
    // 3. Compute Document Frequency (DF) for each term
    // --------------------------------------------------------
    println("Computing document frequencies (how many sentences contain each term)...")

    // Explode to get (sentence_id, term) pairs
    val termDocs = sentencesWithTokens
      .select(col("sentence_id"), explode(col("tokens")).alias("term"))
      .distinct()  // Each term counted once per sentence

    // Count how many sentences contain each term
    val documentFrequencies = termDocs
      .groupBy("term")
      .agg(count("*").alias("df"))

    println(s"Vocabulary size: ${documentFrequencies.count()}")

    // --------------------------------------------------------
    // 4. Compute Inverse Document Frequency (IDF)
    // --------------------------------------------------------
    println("Computing inverse document frequencies...")

    val idfData = documentFrequencies
      .withColumn("idf", log((lit(totalSentences) + 1.0) / (col("df") + 1.0)))
      .select("term", "idf")

    // --------------------------------------------------------
    // 5. Compute TF-IDF for each sentence
    // --------------------------------------------------------
    println("Computing TF-IDF vectors for each sentence...")

    // Explode TF maps to get (sentence_id, term, tf) - only what we need for computation
    val tfExploded = sentencesWithTokens
      .select(
        col("sentence_id"),
        explode(col("tf"))
      )
      .select(
        col("sentence_id"),
        col("key").alias("term"),
        col("value").alias("tf")
      )

    // Join with IDF to compute TF-IDF
    val tfidfScores = tfExploded
      .join(idfData, Seq("term"), "inner")
      .withColumn("tfidf", col("tf") * col("idf"))
      .select("sentence_id", "term", "tfidf")  // Keep only what we need

    // --------------------------------------------------------
    // 6. Aggregate TF-IDF vectors per sentence
    // --------------------------------------------------------
    println("Aggregating TF-IDF vectors per sentence...")

    // Collect TF-IDF scores into a vector per sentence
    val sentenceTFIDF = tfidfScores
      .groupBy("sentence_id")
      .agg(
        collect_list(struct(col("term"), col("tfidf"))).alias("tfidf_vector")
      )

    // Join back with sentence metadata
    val sentencesWithTFIDF = sentencesWithTokens
      .select("sentence_id", "review_id", "asin", "sentence")
      .join(sentenceTFIDF, Seq("sentence_id"), "inner")

    // --------------------------------------------------------
    // 7. Aggregate sentences by review
    // --------------------------------------------------------
    println("Aggregating sentences by review...")

    val reviewsWithSentences = sentencesWithTFIDF
      .groupBy("review_id", "asin")
      .agg(
        collect_list(
          struct(
            col("sentence"),
            col("tfidf_vector")
          )
        ).alias("sentences")
      )

    // --------------------------------------------------------
    // 8. Aggregate reviews by book (ASIN) for final output
    // --------------------------------------------------------
    println("Aggregating reviews by book...")

    val bookReviews = reviewsWithSentences
      .groupBy("asin")
      .agg(
        collect_list(col("sentences")).alias("reviews"),
        count("*").alias("num_reviews")
      )

    val totalBooks = bookReviews.count()
    println(s"Aggregated into $totalBooks books")

    // Print statistics
    println("\n=== Statistics ===")
    println(s"Total sentences with TF-IDF vectors: ${sentencesWithTFIDF.count()}")
    println(s"Total reviews: $totalReviews")
    println(s"Total books: $totalBooks")

    val vocabStats = sentencesWithTokens
      .select(size(col("tokens")).alias("vocab_size"))
      .agg(
        avg("vocab_size").alias("avg_vocab"),
        min("vocab_size").alias("min_vocab"),
        max("vocab_size").alias("max_vocab")
      ).first()

    println(f"Average vocabulary size per sentence: ${vocabStats.getDouble(0)}%.1f")
    println(f"Vocabulary size range per sentence: ${vocabStats.getInt(1)} - ${vocabStats.getInt(2)}")

    val sentenceStats = sentencesWithTFIDF
      .groupBy("review_id")
      .agg(count("*").alias("sentence_count"))
      .agg(
        avg("sentence_count").alias("avg_sentences"),
        min("sentence_count").alias("min_sentences"),
        max("sentence_count").alias("max_sentences")
      ).first()

    println(f"Average sentences per review: ${sentenceStats.getDouble(0)}%.1f")
    println(f"Sentences per review range: ${sentenceStats.getLong(1)} - ${sentenceStats.getLong(2)}")

    val reviewStats = bookReviews
      .agg(
        avg("num_reviews").alias("avg_reviews_per_book"),
        min("num_reviews").alias("min_reviews"),
        max("num_reviews").alias("max_reviews")
      ).first()

    println(f"Average reviews per book: ${reviewStats.getDouble(0)}%.1f")
    println(f"Reviews per book range: ${reviewStats.getLong(1)} - ${reviewStats.getLong(2)}")

    // Show a sample
    println("\n=== Sample (first 3 books) ===")
    bookReviews.select("asin", "num_reviews")
      .show(3, truncate = false)

    // Save results
    println(s"\nSaving TF-IDF vectors to: $outputPath")
    DataWriter.saveAsJson(bookReviews, outputPath)
    println(s"Saved TF-IDF vectors to: $outputPath")

    spark.stop()
  }
}
