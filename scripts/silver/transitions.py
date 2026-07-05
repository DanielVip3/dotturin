from common import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import pandas as pd

STREAM_TIMEOUT_HOURS = 12   # After 12 hour with no data, the stream is considered offline

# Initialize Spark session
spark = get_spark_session("TwitchNoNameSilverTransitions")
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Read data from bronze layer Delta Lake
bronze_df = spark.readStream \
  .format("delta") \
  .load("s3a://twitch-bronze/streams/")

# Transitions table schema
transitions_schema = StructType([
  StructField("stream_id", StringType()),
  StructField("user_name", StringType()),
  StructField("event_type", StringType()), # can be "GAME_CHANGE" or "TITLE_CHANGE"
  StructField("old_value", StringType()),
  StructField("new_value", StringType()),
  StructField("event_ts", TimestampType()),
  StructField("viewer_delta", IntegerType())
])
transitions_columns = [f.name for f in transitions_schema.fields]

# State schema for each stream_id containing the last value of interested fields
state_schema = "last_game string, " \
  "last_title string, " \
  "last_viewers long, " \
  "last_ts timestamp"

def detect_stream_transitions(grouping_key, dfs, state: GroupState):
  stream_id = grouping_key[0]
  events = []

  # If we get no new data after 12 hours, the stream is probably finished. Clear the state.
  if state.hasTimedOut:
    state.remove()
    return []

  for df in dfs:
    df = df.sort_values("ingestion_ts")
    
    for _, row in df.iterrows():
      if not state.exists: # First time this stream was seen
        state.update((row["game_name"], row["title"], row["viewer_count"], row["ingestion_ts"]))
      else: # Stream already ongoing
        old_game, old_title, old_viewers, old_ts = state.get
        
        viewer_delta = row["viewer_count"] - old_viewers
        
        # Case 1: game change event
        if row["game_name"] != old_game:
          events.append([stream_id, row["user_name"], "GAME_CHANGE", str(old_game), str(row["game_name"]), row["ingestion_ts"], viewer_delta])
        
        # Case 2: title change event
        if row["title"] != old_title:
          events.append([stream_id, row["user_name"], "TITLE_CHANGE", str(old_title), str(row["title"]), row["ingestion_ts"], viewer_delta])

        state.update((row["game_name"], row["title"], row["viewer_count"], row["ingestion_ts"]))
  
  state.setTimeoutDuration(STREAM_TIMEOUT_HOURS * 60 * 60 * 1000)

  if events:
    return [pd.DataFrame(events, columns=transitions_columns)]
  else:
    return []

# Transitions table with state (grouped by stream_id)
transitions_df = bronze_df.select(
    "stream_id",
    "user_name",
    "game_name",
    "title",
    "viewer_count",
    "ingestion_ts"
  ) \
  .groupBy("stream_id") \
  .applyInPandasWithState(
    detect_stream_transitions,
    outputStructType=transitions_schema,
    stateStructType=state_schema,
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
  )

# Write to silver layer Delta Lake
query = transitions_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "s3a://twitch-silver/checkpoints/stream_transitions/") \
  .trigger(processingTime="60 seconds") \
  .start("s3a://twitch-silver/stream_transitions/")

query.awaitTermination()