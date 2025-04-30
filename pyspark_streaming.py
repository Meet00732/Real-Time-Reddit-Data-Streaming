import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType,
    DoubleType, IntegerType, FloatType
)
from confluent_kafka.admin import AdminClient
import time
from datetime import datetime

def create_spark_connection():
    return (
        SparkSession.builder
            .master("spark://spark-master:7077")
            .appName("SparkDataStreaming")
            .config(
              "spark.jars.packages",
              # align these versions with your Spark/Hadoop:
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
              "spark.hadoop.fs.s3a.aws.credentials.provider",
              "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
            .getOrCreate()
    )

def connect_to_kafka(spark):
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers","broker:29092")
            .option("subscribe","reddit_data_created")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss","false")
            .load()
    )

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
    exploded = parsed.select(explode(col("subs")).alias("data"))
    return exploded.select(
        col("data.id").alias("id"),
        col("data.title").alias("title"),
        col("data.selftext").alias("selftext"),
        col("data.top_comments").alias("top_comments"),
        col("data.subreddit").alias("subreddit"),
        col("data.created_utc").alias("created_utc"),
        col("data.score").alias("score"),
        col("data.upvote_ratio").alias("upvote_ratio"),
        col("data.num_comments").alias("num_comments"),
        col("data.url").alias("url"),
        col("data.domain").alias("domain"),
        col("data.author").alias("author"),
        col("data.text_length").alias("text_length"),
    )


def wait_for_topic(brokers: str, topic: str, interval: float = 5.0):
    admin = AdminClient({"bootstrap.servers": brokers})
    while True:
        md = admin.list_topics(timeout=10)
        if topic in md.topics and md.topics[topic].error is None:
            print(f"✅ Found topic {topic}, proceeding…")
            return
        print(f"⏳ Topic {topic} not yet available, retrying in {interval}s…")
        time.sleep(interval)


def write_with_timestamp(batch_df, batch_id):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"s3a://{bucket}/reddit/reddit_{ts}.csv"
    (batch_df
        .write
        .mode("append")           # or "overwrite" if you prefer one file per batch
        .option("header", "true")
        .csv(file_path)
    )
    logging.info(f"Wrote batch {batch_id} to {file_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    KAFKA_BOOTSTRAP = "broker:29092"
    REDDIT_TOPIC   = "reddit_data_created"
    wait_for_topic(KAFKA_BOOTSTRAP, REDDIT_TOPIC, interval=5)
    spark = create_spark_connection()
    kafka_df   = connect_to_kafka(spark)
    selection_df = create_selection_df_from_kafka(kafka_df)

    # you can hard-code it or, better, read from ENV:
    bucket = os.getenv("BUCKET_NAME", "reddit-data-streaming-00732")
    output_path     = f"s3a://{bucket}/reddit/"
    checkpoint_path = f"s3a://{bucket}/reddit_checkpoint/"

    logging.info("Starting write to S3 at %s …", output_path)
    query = (
        selection_df.writeStream
            .outputMode("append")
            .foreachBatch(write_with_timestamp)
            .start()
    )
    query.awaitTermination()