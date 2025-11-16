# Amazon Book Review Summarization

A Spark-based project for sampling and preprocessing Amazon book reviews for text summarization tasks.

## Project Overview

A modular Spark application for processing Amazon book reviews with reusable components for text preprocessing, feature extraction, and data I/O. The codebase is organized into packages (`io`, `preprocess`, `features`, `utils`, `jobs`) for better maintainability. See [ARCHITECTURE.md](ARCHITECTURE.md) for implementation details.

**Main Jobs:**
- `jobs.SampleBooks` - Sample ~5K reviews from raw dataset
- `jobs.PreprocessReviews` - Clean and filter text
- `jobs.TFIDFVectorizer` - Compute TF-IDF vectors per book

## Prerequisites

- Scala 2.12.20
- Apache Spark 3.5.x
- sbt 1.11.x
- Java 8+

## Data

Download the Amazon Reviews 2023 dataset from:
https://amazon-reviews-2023.github.io/

For this project, you need the **Books** category reviews. Place the downloaded `Books.jsonl` file in the `data/` directory:

```bash
# Create data directory if it doesn't exist
mkdir -p data

# Download the Books reviews (example - check website for actual download link)
# Place the file as data/Books.jsonl
```

Expected format: One JSON object per line with fields like `text`, `title`, `rating`, `asin`, etc.

## Build

Compile the project using sbt:

```bash
sbt package
```

This creates the JAR file at:
```
target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar
```

## Usage

### 1. Sample Reviews

Sample books to get a target number of reviews. Each sampled book will have 10-200 reviews:

```bash
spark-submit \
  --driver-memory 2g \
  --executor-memory 2g \
  --class jobs.SampleBooks \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  data/Books.jsonl \
  books_sample_10k \
  10000
```

**Arguments:**
- `<inputPath>`: Path to input JSON file (required)
- `[outputPath]`: Output directory (default: `./books_sample_10k`)
- `[targetReviews]`: Target number of reviews to sample (default: `10000`)

**Sampling strategy:**
- Filter books with ≥10 reviews (ensures enough data per book)
- Cap each book at 200 reviews (avoids single book domination)
- Sample books (not individual reviews) until reaching target number of reviews
- Keep ALL reviews for each selected book

### 2. Preprocess Reviews

Clean and filter the sampled reviews:

```bash
spark-submit \
  --class jobs.PreprocessReviews \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  books_sample_10k \
  data/preprocessed
```

**Arguments:**
- `<inputPath>`: Path to sampled reviews directory (required)
- `<outputPath>`: Output directory for preprocessed data (required)

**Preprocessing steps:**
- Remove HTML tags and URLs
- Normalize whitespace
- Filter by length (text: 50-2000 chars)
- Remove empty reviews

### 3. Compute TF-IDF Vectors

Generate TF-IDF vectors from scratch for each book (aggregates all reviews per book):

```bash
spark-submit \
  --class jobs.TFIDFVectorizer \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  data/preprocessed \
  data/tfidf_books
```

**Arguments:**
- `<inputPath>`: Path to preprocessed reviews directory (required)
- `<outputPath>`: Output directory for TF-IDF vectors (required)
- `[stopWordsPath]`: Path to stop words file (optional, default: `data/stopwords.txt`)

**TF-IDF computation:**
- **Document-level**: Each sentence is treated as a separate document
- Reviews are split into sentences using punctuation-based rules
- Tokenization (lowercase, remove punctuation, min length 3)
- Stop word removal (loads from `data/stopwords.txt` - 111 common English words)
- Term Frequency: TF(term) = count(term) / total_terms_in_sentence
- Document Frequency: Count how many sentences contain each term
- Inverse Document Frequency: IDF(term) = log((total_sentences + 1) / (df + 1))
- TF-IDF: TF(term) × IDF(term)
- **Output**: TF-IDF vector computed per sentence, then aggregated by review and book

## Output Formats

### Preprocessed Reviews

Saved as JSON with the following fields:

```json
{
  "asin": "1642937134",
  "text": "Chris hits it out of the park! Thank you...",
  "text_length": 399
}
```

### TF-IDF Vectors (Per Book)

Saved as JSON with reviews containing sentences and their TF-IDF vectors:

```json
{
  "asin": "0007306202",
  "reviews": [
    [
      {
        "sentence": "Great book!",
        "tfidf_vector": [
          {"term": "great", "tfidf": 0.278},
          {"term": "book", "tfidf": 0.370}
        ]
      },
      {
        "sentence": "Highly recommend to everyone.",
        "tfidf_vector": [
          {"term": "highly", "tfidf": 0.412},
          {"term": "recommend", "tfidf": 0.525}
        ]
      }
    ],
    [
      {
        "sentence": "Another review here.",
        "tfidf_vector": [
          {"term": "another", "tfidf": 0.312},
          {"term": "review", "tfidf": 0.198}
        ]
      }
    ]
  ],
  "num_reviews": 15
}
```

**Note**: Each sentence has its own TF-IDF vector. Sentences are grouped by review (arrays of sentences), and reviews are grouped by book (ASIN).

## Example Workflow

```bash
# 1. Build the project
sbt package

# 2. Sample 10K reviews from the full dataset (default target)
spark-submit --driver-memory 2g --executor-memory 2g \
  --class jobs.SampleBooks \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  data/Books.jsonl

# Or specify a custom target (e.g., 5000 reviews)
# spark-submit --driver-memory 2g --executor-memory 2g \
#   --class jobs.SampleBooks \
#   target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
#   data/Books.jsonl books_sample_5k 5000

# 3. Preprocess the sampled reviews
spark-submit \
  --class jobs.PreprocessReviews \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  books_sample_10k \
  data/preprocessed

# 4. Compute TF-IDF vectors
spark-submit \
  --class jobs.TFIDFVectorizer \
  target/scala-2.12/amzn-book-review-summarization_2.12-0.1.jar \
  data/preprocessed \
  data/tfidf

# 5. Check the output
ls -lh data/tfidf/
head -n 1 data/tfidf/part-*.json | python3 -m json.tool
```

## Notes

- **Important**: All job classes are now in the `jobs` package. Use `--class jobs.ClassName` when running with spark-submit
- The sampling job processes ~29M reviews and may take several minutes
- Increase memory allocation for larger datasets using `--driver-memory` and `--executor-memory`
- Output is written as Spark partitioned JSON files (use `coalesce(1)` for single file output)
- TF-IDF is computed from scratch without using MLlib
- Stop words are loaded from `data/stopwords.txt` (111 words) - easily customizable
- **Sampling strategy ensures each book has 10-200 reviews** for meaningful summarization
- **TF-IDF treats each sentence as a document**, computing vectors at sentence-level with IDF based on all sentences in the corpus
- Sentences are grouped by review, then reviews are grouped by book in the final output for downstream summarization tasks
- The modular architecture makes it easy to add new preprocessing steps or feature extraction methods