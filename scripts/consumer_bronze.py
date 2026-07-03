from common import get_spark_session, gbfs_schema
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp, year, month, day

# Initialize Spark session
spark = get_spark_session("DotTurinStreamingConsumerBronze", master="spark://spark-master:7077")

# Flow from Kafka to Spark
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", "dotturin-bike-status") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

# Parse JSON and explode the bike array as rows into the main table
exploded_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_ts") \
  .withColumn("parsed_json", from_json(col("json_payload"), gbfs_schema)) \
  .withColumn("bike", explode(col("parsed_json.data.bikes")))

# Select all the bikes columns, the received timestamp and the last updated timestamp
bronze_df = exploded_df.select(
  col("timestamp"), # this is already a timestamp in microseconds, unlike the others which are in seconds
  to_timestamp(from_unixtime(col("parsed_json.last_updated"))).alias("last_updated"),
  col("bike.*")
) \
  .withColumn("last_reported", to_timestamp(from_unixtime(col("last_reported"))))

# Deduplicate data, as ingestion may include duplicates.
# Since we are in a streaming process, the last_updated timestamp must be saved across triggers
# to check for duplicates. Using withWatermark we specify that it is necessary to store only
# timestamps only for 2 hours, because we assume that after that time at least one request
# will produce fresh data. It is resilient in case of Dott API errors, but only with duplicates
# with less than 2 hours of distance.
bronze_deduplicated_df = bronze_df \
  .withWatermark("last_updated", "2 hours") \
  .dropDuplicates(["bike_id", "last_updated"])

# Extraction of last updated year, month and day columns to partition later
bronze_time_df = bronze_deduplicated_df \
  .withColumn("year", year(col("last_updated"))) \
  .withColumn("month", month(col("last_updated"))) \
  .withColumn("day", day(col("last_updated")))

# Write stream in Delta Lake format in bucket 'dotturin-raw'
query = bronze_time_df.writeStream \
  .outputMode("append") \
  .format("delta") \
  .partitionBy("year", "month", "day") \
  .option("checkpointLocation", "s3a://dotturin-raw/checkpoints/bronze_bikes/") \
  .start("s3a://dotturin-raw/bikes_status/")

query.awaitTermination()