from common import get_spark_session
from pyspark.sql.functions import col, md5, max, avg, countDistinct
from delta.tables import DeltaTable

# Initialize Spark session
spark = get_spark_session("TwitchNoNameGoldHourly")

# Read data from silver layer enriched streams Delta Lake
silver_enriched_df = spark.readStream \
  .format("delta") \
  .load("s3a://twitch-silver/streams_enriched/")

def process_gold_star_schema(batch_df, batch_id):
  """
  Processes the hourly micro-batch to update Dimensions and Fact tables.
  """

  batch_df.persist()
  
  # dim_game Dimension table; compute game_id by hashing game_name
  dim_game_df = batch_df \
    .select("game_name") \
    .distinct() \
    .filter(col("game_name") != "") \
    .withColumn("game_id", md5(col("game_name")))
  
  # If dim_game delta table exists, merge with new data (if not already present, i.e. upsert)
  if DeltaTable.isDeltaTable(spark, "s3a://twitch-gold/dim_game/"):
    delta_game = DeltaTable.forPath(spark, "s3a://twitch-gold/dim_game/")
    delta_game.alias("target").merge(
      dim_game_df.alias("source"),
      "target.game_id = source.game_id"
    ).whenNotMatchedInsertAll().execute()

  else: # Otherwise, create it
    dim_game_df.write \
      .format("delta") \
      .mode("overwrite") \
      .save("s3a://twitch-gold/dim_game/")


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
  

  # Write to gold layer Delta Lake
  fact_game_hourly_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("started_year", "started_month", "started_day") \
    .save("s3a://twitch-gold/fact_game_hourly/")

  batch_df.unpersist()

# Write incrementally (i.e. batch-like)
query = silver_enriched_df.writeStream \
  .outputMode("update") \
  .foreachBatch(process_gold_star_schema) \
  .option("checkpointLocation", "s3a://twitch-gold/checkpoints/fact_game_hourly/") \
  .trigger(availableNow=True) \
  .start()

query.awaitTermination()
spark.stop()