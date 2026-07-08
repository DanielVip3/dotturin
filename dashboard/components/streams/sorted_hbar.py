import polars as pl
import plotly.express as px
from theme import TWITCH_PURPLE

def sorted_hbar(df: pl.DataFrame, x: str, y: str, x_title: str | None = None, y_title: str | None = None, text: str | None = None, text_template: str | None = None):
  """
  Build a horizontal bar graph sorted by highest to lowest values.
  """

  fig = px.bar(
    df.to_pandas(),
    x=x,
    y=y,
    orientation="h",
    text=text,
    color_discrete_sequence=[TWITCH_PURPLE]
  )

  if text_template is not None:
    fig.update_traces(
      textposition="outside", 
      texttemplate=text_template
    )

  fig.update_layout(yaxis={"categoryorder": "total ascending"}, xaxis_title=x_title, yaxis_title=y_title) 


  return fig