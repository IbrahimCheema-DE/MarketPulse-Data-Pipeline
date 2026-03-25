import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import boto3
from io import StringIO
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'ibrahim-data-engineering-bucket')
TODAY = datetime.now().strftime('%Y-%m-%d')
RAW_S3_KEY = f"raw-data/stock_data_{TODAY}.csv"
PROCESSED_S3_KEY = f"processed-data/stock_data_{TODAY}.parquet"
LOCAL_RAW_PATH = f"stock_data_{TODAY}.csv"
LOCAL_PROCESSED_PATH = f"stock_data_{TODAY}.parquet"


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    )


def download_from_s3():
    logger.info(f"Downloading from S3: {RAW_S3_KEY}")
    s3 = get_s3_client()
    s3.download_file(BUCKET_NAME, RAW_S3_KEY, LOCAL_RAW_PATH)
    logger.info(f"✓ Downloaded to: {LOCAL_RAW_PATH}")


def upload_to_s3():
    logger.info(f"Uploading processed data to S3...")
    s3 = get_s3_client()
    
    for root, dirs, files in os.walk(LOCAL_PROCESSED_PATH):
        for file in files:
            if file.endswith('.parquet'):
                local_file_path = os.path.join(root, file)
                s3_file_key = f"processed-data/{TODAY}/{file}"
                s3.upload_file(local_file_path, BUCKET_NAME, s3_file_key)
                logger.info(f"✓ Uploaded: {s3_file_key}")
    
    logger.info(f"✓ All files uploaded to S3 processed-data folder!")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("MarketPulse_Transform") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_raw_data(spark):
    logger.info(f"Reading local CSV: {LOCAL_RAW_PATH}")
    df = spark.read.csv(LOCAL_RAW_PATH, header=True, inferSchema=True)
    logger.info(f"Total rows loaded: {df.count()}")
    return df


def clean_data(df):
    logger.info("Cleaning data...")
    df = df.dropna(subset=['close', 'volume', 'date'])
    df = df.filter(F.col('close') > 0)
    df = df.filter(F.col('volume') > 0)
    logger.info(f"Rows after cleaning: {df.count()}")
    return df


def add_price_change(df):
    logger.info("Adding price_change_pct column...")
    window = Window.partitionBy('symbol').orderBy('date')
    df = df.withColumn('prev_close', F.lag('close', 1).over(window))
    df = df.withColumn(
        'price_change_pct',
        F.round(
            ((F.col('close') - F.col('prev_close')) / F.col('prev_close')) * 100, 2
        )
    )
    df = df.drop('prev_close')
    return df


def add_moving_average(df):
    logger.info("Adding moving_avg_7d column...")
    window = Window.partitionBy('symbol') \
                   .orderBy('date') \
                   .rowsBetween(-6, 0)
    df = df.withColumn('moving_avg_7d', F.round(F.avg('close').over(window), 2))
    return df


def add_volume_category(df):
    logger.info("Adding volume_category column...")
    df = df.withColumn(
        'volume_category',
        F.when(F.col('volume') > 50000000, 'HIGH')
         .when(F.col('volume') > 20000000, 'MEDIUM')
         .otherwise('LOW')
    )
    return df


def save_locally(df):
    logger.info(f"Saving as Parquet locally...")
    df.write.mode('overwrite').parquet(LOCAL_PROCESSED_PATH)
    logger.info(f"✓ Saved locally: {LOCAL_PROCESSED_PATH}")


def cleanup():
    if os.path.exists(LOCAL_RAW_PATH):
        os.remove(LOCAL_RAW_PATH)
        logger.info(f"✓ Cleaned up local CSV")


def main():
    logger.info("=" * 50)
    logger.info("MarketPulse — PySpark Transform Starting")
    logger.info("=" * 50)

    download_from_s3()

    spark = create_spark_session()
    logger.info("✓ Spark Session created!")

    df = read_raw_data(spark)
    df = clean_data(df)
    df = add_price_change(df)
    df = add_moving_average(df)
    df = add_volume_category(df)

    logger.info("Final Data Preview:")
    df.show(20, truncate=False)
    logger.info(f"Columns: {df.columns}")
    logger.info(f"Total rows: {df.count()}")

    save_locally(df)
    upload_to_s3()
    cleanup()

    spark.stop()

    logger.info("=" * 50)
    logger.info(" Step 2 Complete!")
    logger.info(f" S3: s3://{BUCKET_NAME}/{PROCESSED_S3_KEY}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
