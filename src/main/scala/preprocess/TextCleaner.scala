package preprocess

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TextCleaner {

  /**
   * Clean text by removing HTML tags, URLs, and extra whitespace
   * @param text Input text to clean
   * @return Cleaned text
   */
  def clean(text: String): String = {
    if (text == null || text.isEmpty) {
      ""
    } else {
      text
        .replaceAll("<[^>]+>", "")           // Remove HTML tags
        .replaceAll("http\\S+|www\\S+", "")  // Remove URLs
        .replaceAll("\\s+", " ")             // Remove extra whitespace
        .trim()
    }
  }

  /**
   * UDF version of the clean function for use in Spark DataFrames
   */
  val cleanTextUDF: UserDefinedFunction = udf((text: String) => clean(text))
}
