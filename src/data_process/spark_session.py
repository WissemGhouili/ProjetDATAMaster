"""Creates and configures a Spark session."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProjetData").getOrCreate()