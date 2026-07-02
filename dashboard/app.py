from load_data import load_data
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="DotTurin Fleet Dashboard", layout="wide")
st.title("DotTurin - hourly fleet status")

with st.spinner("Loading data..."):
  df = load_data()

if df.is_empty():
  st.warning("No data available.")
  st.stop()


# --- KPIs
st.subheader(f"Current status (last hour)")

latest_data = df.tail(1).to_dicts()[0]

col1, col2, col3 = st.columns(3)
col1.metric("Fleet size", int(latest_data["total_bikes"]))
col2.metric("Available", int(latest_data["available_bikes"]), delta="Ready to use")
col3.metric("Average fuel", f"{float(latest_data['average_fuel_percent']):g}%")

st.markdown("---")


# --- Line plot
st.subheader("Available bikes over time")

fig = px.line(
  df, 
  x="timestamp", 
  y=["available_bikes"],
  labels={"value": "Available bikes", "timestamp": "Date and hour"}
)

fig.update_layout(hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)


st.subheader("Fleet average fuel over time")

fig = px.line(
  df, 
  x="timestamp", 
  y=["average_fuel_percent"],
  range_y=[0, 100],
  labels={"value": "Fuel percentage", "timestamp": "Date and hour"}
)

fig.update_layout(hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)