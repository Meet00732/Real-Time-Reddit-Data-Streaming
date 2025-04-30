import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType,
    DoubleType, IntegerType, FloatType
)
from confluent_kafka.admin import AdminClient
import time

def create_spark_connection():
    return (
        SparkSession.builder
            .master("spark://spark-master:7077")
            .appName("SparkDataStreaming")
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
            .getOrCreate()
    )

def wait_for_topic(brokers: str, topic: str, interval: float = 5.0):
    admin = AdminClient({"bootstrap.servers": brokers})
    while True:
        md = admin.list_topics(timeout=10)
        if topic in md.topics and md.topics[topic].error is None:
            return
        time.sleep(interval)

def create_selection_df_from_kafka(kafka_df):
    schema = ArrayType(
        StructType([
            StructField("id", StringType(), False),
            StructField("title", StringType()),
            StructField("selftext", StringType()),
            StructField("top_comments", ArrayType(StringType())),
            StructField("subreddit", StringType()),
            StructField("created_utc", DoubleType()),
            StructField("score", IntegerType()),
            StructField("upvote_ratio", FloatType()),
            StructField("num_comments", IntegerType()),
            StructField("url", StringType()),
            StructField("domain", StringType()),
            StructField("author", StringType()),
            StructField("text_length", IntegerType()),
        ])
    )
    parsed = (
        kafka_df
          .selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("subs"))
    )
    return parsed \
        .select(explode(col("subs")).alias("data")) \
        .select("data.*")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    KAFKA_BOOTSTRAP = "broker:29092"
    TOPIC          = "reddit_data_created"
    BUCKET         = os.getenv("BUCKET_NAME", "reddit-data-streaming-00732")
    OUTPUT_PATH    = f"s3a://{BUCKET}/reddit/"
    CHECKPOINT     = f"s3a://{BUCKET}/reddit_checkpoint/"

    wait_for_topic(KAFKA_BOOTSTRAP, TOPIC)
    spark     = create_spark_connection()
    kafka_df  = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    selection_df = create_selection_df_from_kafka(kafka_df) \
        .withColumn("date", current_date())

    # --- single-run, JSON write with date‐partitioning ---
    query = (
        selection_df.writeStream
            .format("json")
            .option("path", OUTPUT_PATH)
            .option("checkpointLocation", CHECKPOINT)
            .partitionBy("date")
            .trigger(once=True)           # ← process available data, then stop
            .outputMode("append")
            .start()
    )
    query.awaitTermination()