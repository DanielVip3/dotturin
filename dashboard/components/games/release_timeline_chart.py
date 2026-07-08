import polars as pl
import streamlit as st
import plotly.express as px
from theme import TWITCH_PURPLE

def release_timeline_chart(trend: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Release timeline")

    if trend.is_empty():
      st.info("No release date data available yet.")
      return

    df = trend.to_pandas()

    fig = px.bar(
      data_frame=df,
      x="period",
      y="games_released",
      color_discrete_sequence=[TWITCH_PURPLE],
      title="Games tracked by release period",
      category_orders={"period": df["period"].tolist()},
    )

    fig.update_layout(
      xaxis_title="Release period",
      yaxis_title="Games tracked"
    )
    
    st.plotly_chart(fig, width='stretch')