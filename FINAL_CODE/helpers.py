"""helper functions

Shared preprocessing helpers for the Student Learning Analytics notebooks.

This module contains concise functions that perform dataset loading,
tokenization, stop-word filtering, frequency aggregation and utilities
to prepare data for sentiment scoring and MongoDB export.

Keeping these helpers in a separate file keeps the notebooks readable
and avoids duplicating transformation logic across analysis and pipeline.
"""

from __future__ import annotations


def load_student_dataset(spark, csv_path: str):
    """Load CSV into a Spark DataFrame with header and inferred schema."""
    return spark.read.csv(csv_path, header=True, inferSchema=True)


def tokenize_opinions(df):
    """Tokenize the online_learning_opinion column into token rows.

    Steps:
    - split on whitespace
    - lowercase tokens
    - remove simple punctuation
    - drop empty tokens
    """
    from pyspark.sql.functions import col, explode, lower, regexp_replace, split

    return (
        df.select(
            "respondent_id",
            "online_learning_opinion",
            explode(split(col("online_learning_opinion"), r"\s+")).alias("token"),
        )
        .withColumn("token", lower(col("token")))
        .withColumn("token", regexp_replace(col("token"), r"[,.!?:;]", ""))
        .filter(col("token") != "")
    )


def load_stop_words():
    """Return a set of English stop words extended with domain noise words."""
    import nltk
    from nltk.corpus import stopwords

    nltk.download("stopwords", quiet=True)

    stop_words = set(stopwords.words("english"))
    stop_words.update({"online", "learning"})
    return stop_words


def filter_opinion_tokens(opinion_tokens, stop_words=None):
    """Filter token rows by stop words using a SQL `NOT IN` expression."""
    from pyspark.sql.functions import expr

    if stop_words is None:
        stop_words = load_stop_words()

    stop_words_list = [word.lower() for word in stop_words]
    if not stop_words_list:
        return opinion_tokens

    quoted_words = ", ".join(
        "'" + word.replace("'", "''") + "'" for word in sorted(stop_words_list)
    )
    return opinion_tokens.filter(expr(f"token NOT IN ({quoted_words})"))


def build_word_frequency(opinion_tokens_filtered, total_opinions):
    """Aggregate token counts per distinct respondent and percentage."""
    from pyspark.sql.functions import col, countDistinct, round as spark_round

    return (
        opinion_tokens_filtered.groupBy("token")
        .agg(countDistinct("respondent_id").alias("opinion_count"))
        .withColumn(
            "pct_opinions",
            spark_round(col("opinion_count") / total_opinions * 100, 2),
        )
        .orderBy("opinion_count", ascending=False)
    )


def build_processed_opinions(opinion_tokens_filtered):
    """Group tokens back to respondent level as a set of tokens."""
    from pyspark.sql.functions import collect_set

    return (
        opinion_tokens_filtered.groupBy("respondent_id", "online_learning_opinion")
        .agg(collect_set("token").alias("tokens"))
        .orderBy("respondent_id")
    )


def add_vader_sentiment(processed_opinions):
    """Attach VADER compound score and a simple sentiment label.

    Returns a DataFrame with `sentiment_score` and `sentiment_label` columns.

    The SentimentIntensityAnalyzer is instantiated inside the UDF so each
    Spark worker creates its own instance at execution time. Instantiating it
    outside and closing over it causes pickling failures during task
    serialization.
    """
    from pyspark.sql.functions import col, udf, when
    from pyspark.sql.types import FloatType

    def get_compound(text):
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        if text is None:
            return 0.0
        analyzer = SentimentIntensityAnalyzer()
        return float(analyzer.polarity_scores(text)["compound"])

    vader_udf = udf(get_compound, FloatType())

    return processed_opinions.withColumn(
        "sentiment_score", vader_udf(col("online_learning_opinion"))
    ).withColumn(
        "sentiment_label",
        when(col("sentiment_score") >= 0.05, "positive")
        .when(col("sentiment_score") <= -0.05, "negative")
        .otherwise("neutral"),
    )


def build_text_features(df):
    """Wrapper that produces token tables and word frequency.

    Sentiment scoring is kept as a separate step so the notebook can show
    analysis explicitly before mutation.
    """
    opinion_tokens = tokenize_opinions(df)
    opinion_tokens_filtered = filter_opinion_tokens(opinion_tokens)
    word_freq = build_word_frequency(opinion_tokens_filtered, df.count())
    processed_opinions = build_processed_opinions(opinion_tokens_filtered)
    return opinion_tokens, opinion_tokens_filtered, word_freq, processed_opinions


def build_token_bridge_df(processed_opinions):
    """Explode the token arrays back to one document per respondent-token pair."""
    from pyspark.sql.functions import col, explode

    return processed_opinions.select(
        "respondent_id",
        explode(col("tokens")).alias("token"),
    ).orderBy("respondent_id", "token")
