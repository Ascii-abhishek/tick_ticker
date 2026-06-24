import os

import polars as pl

R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_S3_ENDPOINT = os.environ["R2_S3_ENDPOINT"]
R2_BUCKET_NAME = os.environ["R2_BUCKET_NAME"]
R2_FUTURES_PREFIX = os.getenv("R2_FUTURES_PREFIX", "futures")

# R2 Configuration
storage_options = {
    "aws_access_key_id": R2_ACCESS_KEY_ID,
    "aws_secret_access_key": R2_SECRET_ACCESS_KEY,
    "endpoint_url": R2_S3_ENDPOINT,
    "aws_region": os.getenv("AWS_REGION", "auto"),
}

source = f"s3://{R2_BUCKET_NAME}/{R2_FUTURES_PREFIX}/ohlcv/**/*.parquet"
df = pl.scan_parquet(source, storage_options=storage_options).collect()

ctx = pl.SQLContext(ohlcv_futures=df)
print(ctx.execute("SELECT count(*) FROM ohlcv_futures").collect())
