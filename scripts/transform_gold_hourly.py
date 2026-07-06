from common import get_spark_session
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, md5, max, avg, countDistinct

# Initialize Spark session
spark = get_spark_session("TwitchNoNameGoldHourly")

CH_HOST = "clickhouse"
CH_PORT = "8123" 
CH_USER = os.environ.get("CLICKHOUSE_USER")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CH_DB = os.environ.get("CLICKHOUSE_DB")
JDBC_URL = f"jdbc:ch://{CH_HOST}:{CH_PORT}/{CH_DB}?compress=0"

# Read data from silver layer enriched streams Delta Lake
silver_enriched_df = spark.readStream \
  .format("delta") \
  .load("s3a://twitch-silver/streams_enriched/")

def write_to_clickhouse(df: DataFrame, table: str):
  df.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", table) \
    .option("user", CH_USER) \
    .option("password", CH_PASSWORD) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("batchsize", 10000) \
    .mode("append") \
    .save()

def process_gold(batch_df: DataFrame, _: int):
  """
  Processes the hourly micro-batch to insert into Dimensions and Fact tables.
  """

  batch_df.persist()
  
  # dim_game Dimension table; compute game_id by hashing game_name
  dim_game_df = batch_df \
    .select("game_name") \
    .distinct() \
    .filter(col("game_name") != "") \
    .withColumn("game_id", md5(col("game_name")))

  write_to_clickhouse(dim_game_df, "dim_game")


  # fact_game_hourly Fact table
  # Dimensions: game, date and time
  # Metrics: max viewers, avg viewers and number of unique channels
  fact_game_hourly_df = batch_df \
    .withColumn("game_id", md5(col("game_name"))) \
    .groupBy(
      "game_id", 
      "started_year", 
      "started_month", 
      "started_day", 
      "started_hour"
    ).agg(
      max("viewer_count").alias("max_concurrent_viewers"),
      avg("viewer_count").alias("avg_viewers"),
      countDistinct("stream_id").alias("total_active_channels")
    )

  write_to_clickhouse(fact_game_hourly_df, "fact_game_hourly")
    
  batch_df.unpersist()

# Write incrementally (i.e. batch-like)
query = silver_enriched_df.writeStream \
  .outputMode("update") \
  .foreachBatch(process_gold) \
  .option("checkpointLocation", "s3a://twitch-gold/checkpoints/fact_game_hourly/") \
  .trigger(availableNow=True) \
  .start()

query.awaitTermination()
spark.stop()