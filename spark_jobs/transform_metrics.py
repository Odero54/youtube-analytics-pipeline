from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("YouTubeTransform").getOrCreate()

df = spark.read.csv("data/raw/youtube_data.csv", header=True, inferSchema=True)

df = df.withColumn("engagement_rate", (col("likes") + col("comments")) / col("views"))

df.write.csv("data/processed/youtube_metrics.csv", header=True)
