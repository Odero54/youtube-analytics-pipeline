import findspark
findspark.init()  # Automatically finds your SPARK_HOME

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YouTubeAnalyticsETL") \
    .getOrCreate()
