import polars as pl
import streamlit as st
from types import FunctionType

def events_list(transitions: pl.DataFrame, top_n: int, format_datetime: FunctionType):
  with st.container(border=True):
    EVENT_COLORS = {
      "GAME_CHANGE": "#FF6B6B",
      "TITLE_CHANGE": "#4ECDC4",
      "VIEWER_PEAK": "#FFE66D"
    }
    
    st.subheader(f"{top_n} most recent events")
    
    if not transitions.is_empty():
      recent = transitions.sort("event_ts", descending=True).head(10)
      
      for row in recent.to_dicts():
        event_type = row["event_type"]
        color = EVENT_COLORS.get(event_type, "#999999")
        
        if event_type == "VIEWER_PEAK":
          change_text = f"VIEWERS: {row['old_value']} → {row['new_value']} viewers (+{row['viewer_delta']})"
        elif event_type == "TITLE_CHANGE":
          change_text = f"TITLE: {row['old_value']} \n → {row['new_value']}"
        elif event_type == "GAME_CHANGE":
          change_text = f"GAME: {row['old_value']} \n → {row['new_value']}"

        st.write(
          f"""
          <div style="
            padding: 12px;
            margin: 8px 0;
            border-left: 4px solid {color};
            background-color: rgba(0,0,0,0.02);
            border-radius: 4px;
          ">
            <strong style="color: {color};">{event_type}</strong> · 
            <span style="font-weight: 500;">{row['user_name']}</span> · 
            <span style="color: #666;">{format_datetime(row['event_ts'])}</span><br/>
            <span style="font-size: 0.9em; color: #555;">{change_text}</span>
          </div>
          """,
          unsafe_allow_html=True
        )
    else:
      st.info("No events yet.")