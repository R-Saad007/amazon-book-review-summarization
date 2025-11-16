# Architecture Documentation

## Design Philosophy

This project follows a **modular architecture** that separates concerns into reusable packages. Each package has a single responsibility, making the codebase easier to test, maintain, and extend.

## Package Structure

```
io/         → Data loading and persistence
preprocess/ → Text cleaning and normalization
features/   → Feature extraction (tokenization, TF-IDF)
utils/      → Shared utilities (Spark session management)
jobs/       → Main entry points (orchestration only)
```

## Module Descriptions

### io Package
Handles all data input/output operations:
- **DataLoader**: Load JSON files and stop words
- **DataWriter**: Save DataFrames as JSON or Parquet

### preprocess Package
Text preprocessing utilities:
- **TextCleaner**: Clean text by removing HTML tags, URLs, and normalizing whitespace
- **SentenceSplitter**: Split text into sentences using punctuation-based rules

### features Package
Feature extraction and engineering:
- **Tokenizer**: Convert text to tokens with configurable stop word filtering
- **TFIDFCalculator**: Compute term frequency and TF-IDF scores at sentence level

### utils Package
Common utilities:
- **SparkUtils**: Create and configure SparkSession instances

### jobs Package
Main executable Spark jobs:
- **SampleBooks**: Sample books from the raw Amazon reviews dataset (configurable target size)
- **PreprocessReviews**: Clean and filter reviews, remove HTML/URLs, filter by text length
- **TFIDFVectorizer**: Split reviews into sentences and compute TF-IDF vectors at sentence level

## Data Flow Pipeline

The pipeline consists of three main stages:

### Stage 1: Sampling (SampleBooks)
```
Raw Dataset (29M reviews)
    ↓
Filter books (≥10 reviews per book)
    ↓
Cap reviews per book (max 200)
    ↓
Sample books until target reached (default: 10K reviews)
    ↓
Output: ~10K reviews from 300-500 books
```

### Stage 2: Preprocessing (PreprocessReviews)
```
Sampled Reviews
    ↓
Clean text (remove HTML, URLs, normalize whitespace)
    ↓
Filter by length (50-2000 chars)
    ↓
Remove empty reviews
    ↓
Output: {asin, text, text_length}
```

### Stage 3: TF-IDF Vectorization (TFIDFVectorizer)
```
Preprocessed Reviews
    ↓
Split into sentences (punctuation-based)
    ↓
Tokenize each sentence (remove stop words, min length 3)
    ↓
Compute TF per sentence
    ↓
Compute IDF across all sentences (document frequency)
    ↓
Compute TF-IDF = TF × IDF
    ↓
Group sentences by review
    ↓
Group reviews by book (ASIN)
    ↓
Output: {asin, reviews: [[{sentence, tfidf_vector}]], num_reviews}
```

## TF-IDF Computation Details

### Document Definition
Each **sentence** is treated as a document (not review, not book):
- More granular importance scores
- Better for extractive summarization
- Preserves semantic boundaries

### Formulas
```
TF(term, sentence) = count(term in sentence) / total_terms_in_sentence
DF(term) = number_of_sentences_containing_term
IDF(term) = log((total_sentences + 1) / (DF(term) + 1))
TF-IDF(term, sentence) = TF(term, sentence) × IDF(term)
```

### Example
Given corpus of 10,000 sentences:
- Term "book" appears in 3,000 sentences → IDF = log(10001/3001) = 1.204
- Term "esoteric" appears in 50 sentences → IDF = log(10001/51) = 5.283
- "esoteric" gets higher importance score

