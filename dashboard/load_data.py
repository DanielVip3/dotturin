import os
from dotenv import load_dotenv
import streamlit as st
import polars as pl

load_dotenv()

STORAGE_OPTIONS = {
  "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER"),
  "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD"),
  "AWS_ENDPOINT_URL": "http://localhost:9000",
  "AWS_REGION": "us-east-1",
  "AWS_ALLOW_HTTP": "true"
}

@st.cache_data(ttl=600)
def load_data():
  try:
    # Read Delta Lake gold data from bucket
    df = pl.read_delta(
      "s3://dotturin-processed/hourly_fleet_status/",
      storage_options=STORAGE_OPTIONS
    )
    
    # Create timestamp and sort by it
    df = df.with_columns(
      pl.datetime(
        pl.col("year").cast(pl.Int32), 
        pl.col("month").cast(pl.Int32), 
        pl.col("day").cast(pl.Int32), 
        pl.col("hour").cast(pl.Int32)
      ).alias("timestamp")
    ).sort("timestamp")
    
    # Multiply the fuel percentage (in [0, 1]) by 100
    df = df.with_columns(
      average_fuel_percent = pl.col("average_fuel_percent") * 100
    )

    return df

  except Exception as e:
    st.error(f"Error in data loading: {e}")
    return pl.DataFrame()