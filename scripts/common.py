import os
from dotenv import load_dotenv
import json
from pyspark.sql import SparkSession

load_dotenv()

TOPIC_STREAMS = os.environ.get("TOPIC_STREAMS")
TOPIC_GAMES = os.environ.get("TOPIC_GAMES")


def get_spark_session(app_name: str, master: str | None = None) -> SparkSession:
  """
  Initialize and return a SparkSession pre-configured for Delta Lake and MinIO.
  """

  spark = (
    SparkSession.builder.appName(app_name)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    .config("spark.sql.files.ignoreMissingFiles", "true")
  )

  if master is not None:
    spark = spark.master(master)

  spark = spark.getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  return spark


# Twitch stream API schema
STREAM_AVRO_SCHEMA_DICT = {
  "type": "record",
  "name": "Stream",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "user_name", "type": "string"},
    {"name": "game_name", "type": "string"},
    {"name": "title", "type": "string"},
    {"name": "tags", "type": ["null", {"type": "array", "items": "string"}], "default": None},
    {"name": "viewer_count", "type": "int"},
    {"name": "started_at", "type": "string"},
    {"name": "language", "type": "string"},
    {"name": "thumbnail_url", "type": "string"},
  ],
}

# Twitch game API schema + IGDB game metadata schema
GAME_AVRO_SCHEMA_DICT = {
  "type": "record",
  "name": "Game",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "igdb_id", "type": ["null", "string"], "default": None},
    {
      "name": "igdb_data",
      "type": [
        "null",
        {
          "type": "record",
          "name": "IgdbData",
          "fields": [
            {"name": "id", "type": ["null", "int"], "default": None},
            {"name": "summary", "type": ["null", "string"], "default": None},
            {"name": "total_rating", "type": ["null", "double"], "default": None},
            {"name": "total_rating_count", "type": ["null", "int"], "default": None},
            {"name": "first_release_date", "type": ["null", "long"], "default": None},
            {"name": "storyline", "type": ["null", "string"], "default": None},
            {"name": "url", "type": ["null", "string"], "default": None},
            {
              "name": "themes",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "IdName",
                    "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
                  },
                },
              ],
              "default": None,
            },
            {"name": "player_perspectives", "type": ["null", {"type": "array", "items": "IdName"}], "default": None},
            {"name": "keywords", "type": ["null", {"type": "array", "items": "IdName"}], "default": None},
            {"name": "game_modes", "type": ["null", {"type": "array", "items": "IdName"}], "default": None},
            {
              "name": "platforms",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "Platform",
                    "fields": [
                      {"name": "name", "type": "string"},
                      {"name": "platform_family", "type": ["null", "IdName"], "default": None},
                      {"name": "platform_type", "type": ["null", "IdName"], "default": None},
                    ],
                  },
                },
              ],
              "default": None,
            },
          ],
        },
      ],
      "default": None,
    },
  ],
}

STREAM_AVRO_SCHEMA = json.dumps(STREAM_AVRO_SCHEMA_DICT)
GAME_AVRO_SCHEMA = json.dumps(GAME_AVRO_SCHEMA_DICT)
