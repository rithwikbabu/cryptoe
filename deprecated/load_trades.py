import logging
import os
import sys
from datetime import date, timedelta

import psycopg2  # Install via pip if not already installed
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, IntegerType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("polygon_data_loader.log")
    ]
)
logger = logging.getLogger(__name__)


def create_date_range(start_date: date, end_date: date) -> list[date]:
    """Create a list of dates from start_date to end_date inclusive."""
    delta = (end_date - start_date).days
    return [start_date + timedelta(days=i) for i in range(delta + 1)]


def construct_object_key(dt: date) -> str:
    """Construct the S3 object key based on the given date."""
    return (
        f"global_crypto/trades_v1/{dt.strftime('%Y')}/"
        f"{dt.strftime('%m')}/{dt.strftime('%Y-%m-%d')}.csv.gz"
    )


def configure_spark_session(aws_access_key_id: str, aws_secret_access_key: str) -> SparkSession:
    """Configure and return a Spark session with S3 credentials."""
    spark = SparkSession.builder \
        .appName("PolygonDataLoader") \
        .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.2.23"
    ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .getOrCreate()

    # Reduce noisy Spark logs
    spark.sparkContext.setLogLevel("ERROR")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", "https://files.polygon.io")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    return spark


def upsert_records(df, postgres_conn_str: str):
    """
    Write the DataFrame to a temporary table and then upsert into the target 'trades' table
    using PostgreSQL's ON CONFLICT clause.
    """
    tmp_table = "trades_tmp"

    # Write DataFrame to temporary staging table (overwrite if exists)
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "mypass",
        "driver": "org.postgresql.Driver"
    }
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", tmp_table) \
        .options(**connection_properties) \
        .mode("overwrite") \
        .save()

    # Connect to PostgreSQL to perform the upsert
    try:
        conn = psycopg2.connect(postgres_conn_str)
        cur = conn.cursor()
        merge_sql = f"""
            INSERT INTO trades (ticker, condition_id, exchange_id, trade_id, timestamp, price, size)
            SELECT ticker, condition_id, exchange_id, trade_id, timestamp, price, size
            FROM {tmp_table}
            ON CONFLICT (ticker, trade_id) DO UPDATE
              SET ticker = EXCLUDED.ticker,
                  condition_id = EXCLUDED.condition_id,
                  exchange_id = EXCLUDED.exchange_id,
                  timestamp = EXCLUDED.timestamp,
                  price = EXCLUDED.price,
                  size = EXCLUDED.size;
            DROP TABLE {tmp_table};
        """
        cur.execute(merge_sql)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Upsert completed successfully.")
    except Exception as e:
        logger.exception("Error during upsert operation")


def process_file(dt: date, schema: StructType, spark: SparkSession, postgres_conn_str: str) -> None:
    object_key = construct_object_key(dt)
    s3_path = f"s3a://flatfiles/{object_key}"
    try:
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("compression", "gzip") \
            .schema(schema) \
            .load(s3_path)

        # Rename columns to match the DB schema
        df = df.select(
            col("ticker"),
            col("conditions").alias("condition_id"),
            col("exchange").alias("exchange_id"),
            col("id").alias("trade_id"),
            col("participant_timestamp").alias("timestamp"),
            col("price"),
            col("size")
        )

        # Deduplicate rows based on conflict keys
        df = df.dropDuplicates(["trade_id", "ticker"])

        # Optional: Repartition if necessary
        df = df.repartition(5)

        # Upsert the deduplicated DataFrame
        upsert_records(df, postgres_conn_str)
        logger.info(f"Successfully processed and upserted {object_key}")
    except Exception as e:
        logger.exception(f"Error processing {object_key}")


def main():
    if len(sys.argv) != 3:
        logger.error("Usage: python script.py <start_date> <end_date>")
        sys.exit(1)

    _, start_date_str, end_date_str = sys.argv

    try:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    aws_access_key_id = os.getenv("POLYGON_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("POLYGON_AWS_SECRET_ACCESS_KEY")
    if not aws_access_key_id or not aws_secret_access_key:
        logger.error("AWS credentials not set in environment variables")
        sys.exit(1)

    postgres_conn_str = "dbname=postgres user=postgres password=mypass host=localhost port=5432"

    spark = configure_spark_session(aws_access_key_id, aws_secret_access_key)

    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("conditions", IntegerType(), True),
        StructField("exchange", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("participant_timestamp", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("size", DoubleType(), True)
    ])

    date_range = create_date_range(start_date, end_date)

    for dt in date_range:
        process_file(dt, schema, spark, postgres_conn_str)

    spark.stop()


if __name__ == "__main__":
    main()
