# spark_jobs/transform_youtube_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, current_timestamp, udf
)
from pyspark.sql.types import IntegerType
import re

def parse_duration_to_seconds(duration_str):
    """Parses ISO 8601 duration format to seconds."""
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration_str)
    if not match:
        return 0
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

# Register the UDF
parse_duration_udf = udf(parse_duration_to_seconds, IntegerType())

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TransformYouTubeData") \
        .getOrCreate()

    print("Spark session started.")

    # Load raw YouTube data
    raw_df = spark.read.option("multiline", True).json(
        "/home/george/data_engineering/youtube-analytics-pipeline/data/raw/raw_youtube_data.json"
    )

    print("Raw data loaded.")

    # Flatten and transform required fields
    transformed_df = raw_df.select(
        col("id").alias("video_id"),
        col("snippet.title").alias("title"),
        col("snippet.description").alias("description"),
        to_timestamp(col("snippet.publishedAt")).alias("published_at"),
        col("statistics.viewCount").cast("int").alias("view_count"),
        col("statistics.likeCount").cast("int").alias("like_count"),
        col("statistics.commentCount").cast("int").alias("comment_count"),
        col("contentDetails.duration").alias("raw_duration"),
        col("snippet.tags").alias("tags"),
        col("snippet.categoryId").alias("category_id"),
        col("snippet.channelTitle").alias("channel_title"),
        current_timestamp().alias("fetched_at")
    )

    # Calculate engagement rate safely
    transformed_df = transformed_df.withColumn(
        "engagement_rate",
        when(col("view_count") > 0,
             (col("like_count") + col("comment_count")) / col("view_count")
        ).otherwise(0.0)
    )

    # Convert ISO 8601 durations to seconds
    transformed_df = transformed_df.withColumn(
        "duration",
        parse_duration_udf(col("raw_duration"))
    ).drop("raw_duration")

    # Show result
    transformed_df.show(truncate=False)
    
    # Save the output
    transformed_df.write.mode("overwrite").parquet("/home/george/data_engineering/youtube-analytics-pipeline/data/processed/transformed_youtube_data.parquet")

    print("Transformation completed.")

if __name__ == "__main__":
    main()
