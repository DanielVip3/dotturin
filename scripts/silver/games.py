from common import get_spark_session
from pyspark.sql.functions import col, from_unixtime, array_distinct

# Initialize Spark session
spark = get_spark_session("TwitchySilverGames")

# Read data from bronze layer Delta Lake
bronze_df = spark.readStream.format("delta").load("s3a://twitch-bronze/games/")

# Parse timestamps and flatten IGDB arrays
silver_games_df = bronze_df.select(
  col("ingestion_ts"),
  col("game_id"),
  col("game_name"),
  col("igdb_id"),
  col("igdb_data.summary"),
  col("igdb_data.total_rating"),
  col("igdb_data.total_rating_count"),
  col("igdb_data.storyline"),
  col("igdb_data.url"),
  # Parse Unix epoch time
  from_unixtime(col("igdb_data.first_release_date")).cast("timestamp").alias("first_release_date"),
  # Flatten 1-level arrays of structs (e.g., [{"name": "Action"}] -> ["Action"])
  col("igdb_data.themes.name").alias("themes"),
  col("igdb_data.player_perspectives.name").alias("player_perspectives"),
  col("igdb_data.keywords.name").alias("keywords"),
  col("igdb_data.game_modes.name").alias("game_modes"),
  col("igdb_data.platforms.name").alias("platforms"),
  # Flatten and use array_distinct to remove duplicates
  array_distinct(col("igdb_data.platforms.platform_family.name")).alias("platform_families"),
  array_distinct(col("igdb_data.platforms.platform_type.name")).alias("platform_types"),
)

# Write to silver layer Delta Lake
query = (
  silver_games_df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3a://twitch-silver/checkpoints/games/")
  .trigger(processingTime="60 seconds")
  .start("s3a://twitch-silver/games/")
)

query.awaitTermination()
