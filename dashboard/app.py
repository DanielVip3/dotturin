import streamlit as st
from load_data import load_data
import pydeck as pdk
import polars as pl

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

# Compute paths from trips
paths = []
for r in df.to_dicts():
  # Blue for scooters, green for bicycles
  color = [0, 120, 255, 180] if "scooter" in r["vehicle_type_id"] else [0, 200, 100, 180]

  paths.append({
    "trip_id": r["bike_id"],
    "path": [
      [r["start_lon"], r["start_lat"]],
      [r["end_lon"], r["end_lat"]],
    ],
    "timestamp_start": r["start_ts"],
    "timestamp_end": r["end_ts"],
    "color": color
  })

st.subheader("Trips map")
layer = pdk.Layer(
  "PathLayer",
  data=paths,
  get_path="path",
  get_width=5,
  get_color="color",
  width_min_pixels=2,
)

lats = [p["path"][0][1] for p in paths] + [p["path"][1][1] for p in paths]
lons = [p["path"][0][0] for p in paths] + [p["path"][1][0] for p in paths]
view_state = pdk.ViewState(
  latitude=sum(lats) / len(lats),
  longitude=sum(lons) / len(lons),
  zoom=12,
)

st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))