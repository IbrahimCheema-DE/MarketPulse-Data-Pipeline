import os
import logging
import boto3
import pandas as pd
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'ibrahim-data-engineering-bucket')
TODAY = datetime.now().strftime('%Y-%m-%d')

MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_DB = os.getenv('MYSQL_DB', 'marketpulse_db')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


def get_mysql_connection():
    logger.info("Connecting to MySQL...")
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    logger.info("✓ MySQL connected!")
    return conn


def download_raw_data():
    logger.info("Downloading raw CSV from S3...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    )

    raw_key = f"raw-data/stock_data_{TODAY}.csv"
    local_path = f"/tmp/stock_data_{TODAY}.csv"

    s3.download_file(BUCKET_NAME, raw_key, local_path)
    logger.info(f"✓ Downloaded: {raw_key}")

    df = pd.read_csv(local_path)
    os.remove(local_path)
    logger.info(f"Total rows: {len(df)}")
    return df


def transform_data(df):
    df = df.dropna(subset=['close', 'volume', 'date'])
    df = df[df['close'] > 0]
    df = df[df['volume'] > 0]

    df = df.sort_values(['symbol', 'date'])

    df['prev_close'] = df.groupby('symbol')['close'].shift(1)
    df['price_change_pct'] = round(
        ((df['close'] - df['prev_close']) / df['prev_close']) * 100, 2
    )
    df = df.drop('prev_close', axis=1)

    df['moving_avg_7d'] = df.groupby('symbol')['close'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean().round(2)
    )

    df['volume_category'] = df['volume'].apply(
        lambda v: 'HIGH' if v > 50000000 else ('MEDIUM' if v > 20000000 else 'LOW')
    )

    logger.info(f"Rows after transform: {len(df)}")
    return df


def load_data_to_mysql(conn, df):
    logger.info(f"Loading {len(df)} rows to MySQL...")
    cursor = conn.cursor()

    cursor.execute("DELETE FROM stock_prices WHERE DATE(date) = %s", (TODAY,))

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stock_prices
            (date, symbol, open, high, low, close, volume,
             price_change_pct, moving_avg_7d, volume_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(row['date']),
            str(row['symbol']),
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            int(row['volume']),
            float(row['price_change_pct']) if pd.notna(row['price_change_pct']) else None,
            float(row['moving_avg_7d']),
            str(row['volume_category'])
        ))

    conn.commit()
    logger.info(f"✓ {len(df)} rows loaded!")


def verify_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_prices")
    total = cursor.fetchone()[0]
    logger.info(f"Total rows in MySQL: {total}")

    cursor.execute("""
        SELECT symbol, ROUND(MAX(close), 2), ROUND(MIN(close), 2), ROUND(AVG(close), 2)
        FROM stock_prices
        GROUP BY symbol
        ORDER BY symbol
    """)
    rows = cursor.fetchall()
    print("\n📊 Stock Summary:")
    print(f"{'Symbol':<10} {'Max':>10} {'Min':>10} {'Avg':>10}")
    print("-" * 45)
    for row in rows:
        print(f"{row[0]:<10} {row[1]:>10} {row[2]:>10} {row[3]:>10}")


def main():
    logger.info("=" * 50)
    logger.info("MarketPulse — MySQL Loading Starting")
    logger.info("=" * 50)

    df = download_raw_data()
    df = transform_data(df)
    conn = get_mysql_connection()
    load_data_to_mysql(conn, df)
    verify_data(conn)
    conn.close()

    logger.info("=" * 50)
    logger.info("✅ Step 3 Complete!")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()