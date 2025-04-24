import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
    logging.info("Keyspace created successfully!")


def create_table(session):
    # Create table
    pass

def insert_data(session, **kwargs):
    # Insert data
    pass

def create_spark_connection():
    # Create spark connection
    try:
        s_conn = SparkSession.Builder \
                .appName("SparkDataStreaming") \
                .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1," "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                .config('spark.cassandra.connection.host', 'broker') \
                .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Could not create spark session due to: {e}")

    return s_conn



def create_cassandra_connection():
    # Create cassandra connection
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


if __name__ == "__main__":
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace()
            create_table()

