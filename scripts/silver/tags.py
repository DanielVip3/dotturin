from common import get_spark_session
from pyspark.sql.functions import col, explode, lower, trim

# Initialize Spark session
spark = get_spark_session("TwitchySilverTags")

# Read data from bronze layer Delta Lake
bronze_df = spark.readStream.format("delta").load("s3a://twitch-bronze/streams/")

# Explode tags array into its own table (primary keys are stream_id and ingestion_ts)
tags_df = (
  bronze_df.select("stream_id", "ingestion_ts", "tags")
  .withColumn("tag_name", explode(col("tags")))
  .withColumn("tag_name", lower(trim(col("tag_name"))))
  .select("stream_id", "ingestion_ts", "tag_name")
)

# Write to silver layer Delta Lake
query = (
  tags_df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3a://twitch-silver/checkpoints/stream_tags/")
  .trigger(processingTime="60 seconds")
  .start("s3a://twitch-silver/stream_tags/")
)

query.awaitTermination()
