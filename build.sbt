name := "amzn-book-review-summarization"

version := "0.1"

scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)
