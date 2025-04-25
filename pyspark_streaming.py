import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType,
    DoubleType, IntegerType, FloatType
)

def create_spark_connection():
    return (
        SparkSession.builder
            .master("spark://spark-master:7077")
            .appName("SparkDataStreaming")
            # pull in Hadoop AWS + Kafka support
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )
            # S3A filesystem impl
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # use the EC2 instance profile credentials
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider"
            )
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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
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
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .trigger(once=True)
            .start()
    )
    query.awaitTermination()