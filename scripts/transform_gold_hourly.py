from common import get_spark_session
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, sum, count, countDistinct, expr, concat, lpad

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

  clean_batch_df = batch_df.filter(
    col("game_name").isNotNull() & (expr("trim(game_name)") != "")
  )
  clean_batch_df.persist()

  # dim_game Dimension table
  dim_game_df = batch_df \
    .select("game_name") \
    .distinct() \
    .withColumn("game_id", expr("xxhash64(game_name)")) # integer hash of game_name

  write_to_clickhouse(dim_game_df, "dim_game")


  # dim_date Dimension table
  dim_date_df = batch_df \
    .select(
      col("started_year").alias("date_year"),
      col("started_month").alias("date_month"),
      col("started_day").alias("date_day")
    ) \
    .distinct() \
    .withColumn("date_id", # number like 20260707
      concat(
        col("date_year"), 
        lpad(col("date_month"), 2, "0"), 
        lpad(col("date_day"), 2, "0")
      ).cast("long")
    )

  write_to_clickhouse(dim_date_df, "dim_date")


  # fact_game_hourly Fact table
  # Dimensions: game, date and time
  # Metrics: max viewers, avg viewers and number of unique channels
  fact_game_hourly_df = batch_df \
    .withColumn("game_id", expr("xxhash64(game_name)")) \
    .withColumn("date_id",
      concat(
        col("started_year"),
        lpad(col("started_month"), 2, "0"),
        lpad(col("started_day"), 2, "0")
      ).cast("long")
    ) \
    .groupBy(
      "game_id",
      "date_id",
      col("started_hour").alias("time_hour")
    ).agg(
      max("viewer_count").alias("max_viewers"),
      sum("viewer_count").alias("sum_viewers"),
      count("viewer_count").alias("count_observations"),
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