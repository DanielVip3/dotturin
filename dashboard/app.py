import streamlit as st
from load_data import load_data

st.set_page_config(page_title="DotTurin Dashboard", layout="wide")

st.title("DotTurin - Trips")

with st.spinner("Loading trips..."):
  df = load_data()

if df.is_empty():
  st.warning("No data available.")
  st.stop()

rows = df.to_dicts()

st.success(f"Loaded {len(rows)} trips")

st.subheader("Trips preview")
st.dataframe(df)