from common import get_spark_session
from pyspark.sql.functions import (
  col,
  regexp_replace,
  unix_timestamp,
  year,
  month,
  hour,
  day,
)

# Initialize Spark session
spark = get_spark_session("TwitchNoNameSilverStreams")

# Read data from bronze layer Delta Lake
bronze_df = spark.readStream.format("delta").load("s3a://twitch-bronze/streams/")

# Enrich streams by adding year, month, day and hour it started, completing placeholder thumbnail URL, and pre-computing seconds of uptime
enriched_df = (
  bronze_df.drop("tags")
  .withColumn("started_year", year(col("started_at")))
  .withColumn("started_month", month(col("started_at")))
  .withColumn("started_day", day(col("started_at")))
  .withColumn("started_hour", hour(col("started_at")))
  .withColumn(
    "thumbnail_url_1080p",
    regexp_replace(col("thumbnail_url"), "\\{width\\}x\\{height\\}", "1920x1080"),
  )
  .withColumn(
    "stream_time_seconds",
    unix_timestamp(col("ingestion_ts")) - unix_timestamp(col("started_at")),
  )
)
# Write to silver layer Delta Lake
query = (
  enriched_df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3a://twitch-silver/checkpoints/streams/")
  .trigger(processingTime="60 seconds")
  .start("s3a://twitch-silver/streams/")
)

query.awaitTermination()
