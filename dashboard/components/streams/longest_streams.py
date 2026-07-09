import streamlit as st
import polars as pl
import plotly.express as px
from theme import TWITCH_PURPLE


def longest_streams(longest_df: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Longest active streams right now")

    fig = px.bar(
      longest_df.to_pandas(),
      x="duration_hours",
      y="user_name",
      orientation="h",
      hover_data={
        "title": True,
        "started_at": True,
        "duration_hours": ":.2f",  # format hours to 2 decimal places
      },
      labels={
        "duration_hours": "Duration (hours)",
        "user_name": "Streamer",
        "title": "Title",
        "started_at": "Started at",
      },
      color_discrete_sequence=[TWITCH_PURPLE],
    )

    fig.update_layout(yaxis={"categoryorder": "total ascending"})

    st.plotly_chart(fig, width="stretch")
