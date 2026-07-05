import polars as pl
import streamlit as st

def kpis(live: pl.DataFrame, latest: pl.DataFrame):
  with st.container(border=True):
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Unique live streams (entire history)", live["stream_id"].n_unique())
    col2.metric("Unique streamers (entire history)", live["user_name"].n_unique() if not live.is_empty() else 0)
    col3.metric("Current viewers (among top 100)", f"{latest['viewer_count'].sum():,}" if not live.is_empty() else 0)
    col4.metric("Avg viewers/stream (among top 100)", f"{latest['viewer_count'].mean():.0f}" if not live.is_empty() else 0)