package features

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Tokenizer {

  /**
   * Tokenize text into words, applying filtering rules
   * @param text Input text
   * @param stopWords Set of stop words to filter out
   * @param minTokenLength Minimum length for a token to be included (default: 3)
   * @return Array of tokens
   */
  def tokenize(text: String, stopWords: Set[String], minTokenLength: Int = 3): Array[String] = {
    if (text == null || text.isEmpty) {
      Array.empty[String]
    } else {
      text
        .toLowerCase()
        .replaceAll("[^a-z0-9\\s]", " ")  // Keep only alphanumeric and spaces
        .split("\\s+")                      // Split on whitespace
        .filter(_.length >= minTokenLength) // Minimum token length
        .filter(!stopWords.contains(_))     // Remove stop words
    }
  }

  /**
   * Create a UDF for tokenization with specific stop words
   * @param stopWords Set of stop words to filter out
   * @param minTokenLength Minimum length for a token (default: 3)
   * @return UserDefinedFunction for use in Spark DataFrames
   */
  def tokenizeUDF(stopWords: Set[String], minTokenLength: Int = 3): UserDefinedFunction = {
    udf((text: String) => tokenize(text, stopWords, minTokenLength))
  }
}
