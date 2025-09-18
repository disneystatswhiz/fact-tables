# get.py
import os
import pandas as pd  # pip install pandas pyarrow s3fs

S3_FACT_URI = os.getenv(
    "S3_FACT_URI",
    "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet"
)

print(f"Reading Parquet file from: {S3_FACT_URI}")

# read single parquet file from S3
df = pd.read_parquet(S3_FACT_URI, engine="pyarrow")

print(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
print(df.head())
print("\nDataFrame info:")
print(df.info())
print("\nFirst 5 rows:")
print(df.head())