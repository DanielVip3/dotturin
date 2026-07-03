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
    df = pl.read_delta(
      "s3://dotturin-processed/trips/",
      storage_options=STORAGE_OPTIONS
    )

    return df

  except Exception as e:
    st.error(f"Error loading data: {e}")
    return pl.DataFrame()