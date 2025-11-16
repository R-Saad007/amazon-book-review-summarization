package features

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TFIDFCalculator {

  /**
   * Compute term frequency for a document
   * @param tokens Array of tokens from the document
   * @return Map of term to its frequency (normalized by document length)
   */
  def computeTermFrequency(tokens: Array[String]): Map[String, Double] = {
    if (tokens.isEmpty) {
      Map.empty[String, Double]
    } else {
      val totalTerms = tokens.length.toDouble
      tokens
        .groupBy(identity)
        .mapValues(_.length.toDouble / totalTerms)
    }
  }

  /**
   * UDF version of computeTermFrequency for use in Spark DataFrames
   */
  val termFrequencyUDF: UserDefinedFunction = udf((tokens: Seq[String]) =>
    computeTermFrequency(tokens.toArray)
  )

  /**
   * Compute inverse document frequency
   * @param totalDocuments Total number of documents in the corpus
   * @param documentFrequency Number of documents containing the term
   * @return IDF score
   */
  def computeIDF(totalDocuments: Long, documentFrequency: Long): Double = {
    math.log((totalDocuments + 1.0) / (documentFrequency + 1.0))
  }
}
