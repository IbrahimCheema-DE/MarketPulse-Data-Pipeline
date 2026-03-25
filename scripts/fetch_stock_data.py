import yfinance as yf
import pandas as pd
import boto3
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STOCKS = ['AAPL', 'GOOGL', 'AMZN', 'MSFT']
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'ibrahim-data-engineering-bucket')
RAW_FOLDER = 'raw-data'


def fetch_stock_data(symbols: list, period: str = '5d') -> pd.DataFrame:
    all_data = []
    
    for symbol in symbols:
        try:
            logger.info(f"Fetching data for {symbol}...")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval='1d')
            
            if df.empty:
                logger.warning(f"No data found for {symbol}, skipping...")
                continue
            
            df = df.reset_index()
            df['symbol'] = symbol
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df = df[['Date', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            
            all_data.append(df)
            logger.info(f"✓ {symbol}: {len(df)} rows fetched")
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No data fetched for any symbol!")
    
    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined_df)}")
    return combined_df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    original_rows = len(df)
    
    df = df.dropna(subset=['close', 'volume', 'date'])
    df = df[df['close'] > 0]
    df = df[df['volume'] > 0]
    
    today = datetime.now().date()
    df = df[df['date'] <= today]
    
    removed = original_rows - len(df)
    if removed > 0:
        logger.warning(f"Removed {removed} invalid rows during validation")
    
    return df


def upload_to_s3(df: pd.DataFrame, bucket: str, folder: str) -> str:
    today = datetime.now().strftime('%Y-%m-%d')
    filename = f"stock_data_{today}.csv"
    s3_key = f"{folder}/{filename}"
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    )
    
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    s3_path = f"s3://{bucket}/{s3_key}"
    logger.info(f"✓ Data uploaded to: {s3_path}")
    return s3_path


def main():
    logger.info("=" * 50)
    logger.info("MarketPulse — Stock Data Pipeline Starting")
    logger.info("=" * 50)
    
    logger.info("Step 1: Fetching stock data from Yahoo Finance...")
    raw_df = fetch_stock_data(STOCKS, period='5d')
    
    print("\n Raw Data Preview:")
    print(raw_df.head(10).to_string(index=False))
    print(f"\nShape: {raw_df.shape}")
    print(f"Stocks: {raw_df['symbol'].unique()}")
    
    logger.info("Step 2: Validating data quality...")
    clean_df = validate_data(raw_df)
    
    logger.info("Step 3: Uploading to S3 raw layer...")
    s3_path = upload_to_s3(clean_df, BUCKET_NAME, RAW_FOLDER)
    
    logger.info("=" * 50)
    logger.info(f" Pipeline Step 1 Complete!")
    logger.info(f" File saved at: {s3_path}")
    logger.info(f" Total records: {len(clean_df)}")
    logger.info("=" * 50)
    
    return clean_df


if __name__ == "__main__":
    main()