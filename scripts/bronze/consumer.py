from common import (
  get_spark_session,
  STREAM_AVRO_SCHEMA,
  GAME_AVRO_SCHEMA,
  TOPIC_STREAMS,
  TOPIC_GAMES,
)
from pyspark.sql.functions import col, expr, year, month, day, to_timestamp
from pyspark.sql.avro.functions import from_avro

# Initialize Spark session
spark = get_spark_session("TwitchyStreamingConsumerBronze", master="spark://spark-master:7077")

# -- STREAMS TOPIC
# Flow from Kafka to Spark
kafka_streams_df = (
  spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "kafka:29092")
  .option("subscribe", TOPIC_STREAMS)
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
)

# Strip the  Confluent header (5 bytes), deserialize Avro and select all the stream columns and the ingestion timestamp
bronze_streams_df = (
  kafka_streams_df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
  .withColumn("stream", from_avro(col("fixed_value"), STREAM_AVRO_SCHEMA))
  .select(
    col("timestamp").alias("ingestion_ts"),
    col("stream.id").alias("stream_id"),
    col("stream.user_name"),
    col("stream.game_name"),
    col("stream.title"),
    col("stream.tags"),
    col("stream.viewer_count"),
    to_timestamp(col("stream.started_at")).alias("started_at"),
    col("stream.language"),
    col("stream.thumbnail_url"),
  )
)

# Extraction of ingestion year, month and day columns to partition later
bronze_streams_time_df = (
  bronze_streams_df.withColumn("year", year(col("ingestion_ts")))
  .withColumn("month", month(col("ingestion_ts")))
  .withColumn("day", day(col("ingestion_ts")))
)

# Write stream in Delta Lake format in bucket 'twitch-bronze'
query = (
  bronze_streams_time_df.writeStream.outputMode("append")
  .format("delta")
  .partitionBy("year", "month", "day")
  .option("checkpointLocation", "s3a://twitch-bronze/checkpoints/streams/")
  .start("s3a://twitch-bronze/streams/")
)


# -- GAMES TOPIC
kafka_games_df = (
  spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "kafka:29092")
  .option("subscribe", TOPIC_GAMES)
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
)

# Strip the Confluent header (5 bytes), deserialize Avro, select high-level columns and keep the entire IGDB data struct raw and nested
bronze_games_df = (
  kafka_games_df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
  .withColumn("game", from_avro(col("fixed_value"), GAME_AVRO_SCHEMA))
  .select(
    col("timestamp").alias("ingestion_ts"),
    col("game.id").alias("game_id"),
    col("game.name").alias("game_name"),
    col("game.igdb_id"),
    col("game.igdb_data"),
  )
)

# Write stream in Delta Lake format in bucket 'twitch-bronze' (not partitioned)
query = (
  bronze_games_df.writeStream.outputMode("append")
  .format("delta")
  .option("checkpointLocation", "s3a://twitch-bronze/checkpoints/games/")
  .start("s3a://twitch-bronze/games/")
)


spark.streams.awaitAnyTermination()
