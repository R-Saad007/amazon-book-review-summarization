package preprocess

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SentenceSplitter {

  /**
   * Split text into sentences using simple punctuation-based rules
   * @param text Input text
   * @return Array of sentences
   */
  def splitSentences(text: String): Array[String] = {
    if (text == null || text.isEmpty) {
      Array.empty[String]
    } else {
      text
        // Split on sentence-ending punctuation followed by space
        .split("(?<=[.!?])\\s+")
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(_.length >= 10)  // Filter out very short fragments
    }
  }

  /**
   * UDF version for use in Spark DataFrames
   */
  val splitSentencesUDF: UserDefinedFunction = udf((text: String) => splitSentences(text))
}
