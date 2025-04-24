import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType,
    DoubleType, IntegerType, FloatType
)


def create_keyspace(session):
    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
    logging.info("Keyspace created successfully!")


def create_table(session):
    # Create table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.reddit_submissions (
        id                text PRIMARY KEY,
        title             text,
        selftext          text,
        top_comments      list<text>,
        subreddit         text,
        created_utc       double,
        score             int,
        upvote_ratio      float,
        num_comments      int,
        url               text,
        domain            text,
        author            text,
        text_length       int
    );
    """)
    print("Table spark_streams.reddit_submissions created successfully!")

def insert_data(session, **kwargs):
    # Insert data
    """
    Insert one Reddit submission into cassandra.spark_streams.reddit_submissions.
    Expects kwargs with keys:
      id, title, selftext, top_comments, subreddit, created_utc,
      score, upvote_ratio, num_comments, url, domain, author, text_length
    """
    logging.info("Inserting Reddit submission %s …", kwargs.get("id"))

    # unpack all the fields from kwargs (with sensible defaults)
    submission_id   = kwargs.get("id")
    title           = kwargs.get("title", "")
    selftext        = kwargs.get("selftext", "")
    top_comments    = kwargs.get("top_comments", [])    # list<text>
    subreddit       = kwargs.get("subreddit", "")
    created_utc     = kwargs.get("created_utc", 0.0)
    score           = kwargs.get("score", 0)
    upvote_ratio    = kwargs.get("upvote_ratio", 0.0)
    num_comments    = kwargs.get("num_comments", 0)
    url             = kwargs.get("url", "")
    domain          = kwargs.get("domain", "")
    author          = kwargs.get("author", "")
    text_length     = kwargs.get("text_length", 0)

    try:
        session.execute("""
            INSERT INTO spark_streams.reddit_submissions (
                id,
                title,
                selftext,
                top_comments,
                subreddit,
                created_utc,
                score,
                upvote_ratio,
                num_comments,
                url,
                domain,
                author,
                text_length
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            submission_id,
            title,
            selftext,
            top_comments,
            subreddit,
            created_utc,
            score,
            upvote_ratio,
            num_comments,
            url,
            domain,
            author,
            text_length
        ))
        logging.info("Inserted submission %s into cassandra", submission_id)
    except Exception as e:
        logging.error("Failed to insert submission %s: %s", submission_id, e)
        raise


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                    .format('kafka') \
                    .option('kafka.bootstrap.servers', 'broker:29092') \
                    .option('subscribe', 'reddit_data_created') \
                    .option('startingOffsets', 'earliest') \
                    .load()

        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_spark_connection():
    # Create spark connection
    try:
        s_conn = SparkSession.builder \
                .appName("SparkDataStreaming") \
                .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1," "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                .config('spark.cassandra.connection.host', 'cassandra') \
                .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Could not create spark session due to: {e}")
        return None



def create_cassandra_connection():
    # Create cassandra connection
    try:
        cluster = Cluster(['cassandra'])
        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    # Define an ArrayType of Structs matching your Reddit submission fields
    submissions_schema = ArrayType(
        StructType([
            StructField("id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("selftext", StringType(), True),
            StructField("top_comments", ArrayType(StringType()), True),
            StructField("subreddit", StringType(), True),
            StructField("created_utc", DoubleType(), True),
            StructField("score", IntegerType(), True),
            StructField("upvote_ratio", FloatType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("author", StringType(), True),
            StructField("text_length", IntegerType(), True),
        ])
    )

    # 1) cast the binary Kafka value to string and parse it as JSON array
    parsed = (
        spark_df
          .selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), submissions_schema).alias("subs"))
    )

    # 2) explode the array so each element becomes its own row
    exploded = parsed.select(explode(col("subs")).alias("data"))

    # 3) project out all the individual fields
    sel = exploded.select(
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
        col("data.text_length").alias("text_length")
    )

    return sel


if __name__ == "__main__":
    # Create spark connection
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        # Connect kafka with spark connection
        df = connect_to_kafka(spark_connection)
        selection_df = create_selection_df_from_kafka(df)
        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)
            # insert_data()

            logging.info("Streaming is being started...")

            streaming_query = (selection_df
                                .writeStream
                                .format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'reddit_submissions')
                                .start())


            streaming_query.awaitTermination()
