import polars as pl
import streamlit as st
import plotly.express as px

def viewer_trend(streams: pl.DataFrame, top_streamers: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Viewer trend over time for top streams")

    top_users = top_streamers["user_name"].to_list()
    trend_df = streams \
      .filter(pl.col("user_name") \
      .is_in(top_users)) \
      .sort("ingestion_ts")

    fig = px.line(
      trend_df.to_pandas(),
      x="ingestion_ts",
      y="viewer_count",
      color="user_name",
      labels={
        "user_name": "Streamer",
        "ingestion_ts": "Time",
        "viewer_count": "Number of viewers"
      }
    )
    fig.update_layout(xaxis_title="Time", yaxis_title="Number of viewers")

    st.plotly_chart(fig, width="stretch")