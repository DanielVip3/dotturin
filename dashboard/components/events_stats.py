import polars as pl
import streamlit as st
import plotly.express as px

def events_stats(transitions: pl.DataFrame, top_n: int):
  with st.container(border=True):
    st.subheader("Stream events")

    if not transitions.is_empty():
      c7, c8 = st.columns(2)

      with c7:
        st.caption("Event types")
        event_counts = transitions.group_by("event_type").agg(pl.len().alias("events"))
        fig = px.pie(event_counts.to_pandas(), names="event_type", values="events", hole=0.4)
        st.plotly_chart(fig, width="stretch")

      with c8:
        st.caption("Biggest recent viewer peaks")
        peaks = (
          transitions.filter(pl.col("event_type") == "VIEWER_PEAK")
          .sort("viewer_delta", descending=True)
          .head(top_n)
          .select("user_name", "old_value", "new_value", "viewer_delta", "event_ts")
        )
        st.dataframe(peaks.to_pandas(), width="stretch")
    else:
      st.info("No transition events yet.")