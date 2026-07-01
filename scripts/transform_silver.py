from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, BooleanType, ArrayType

load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
  .appName("DotTurinTransformBronzeToSilver") \
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
  .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
  .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
  .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \
  .config("spark.sql.files.ignoreMissingFiles", "true") \
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("[*] Reading raw data from bucket...")

bronze_df = spark.read.parquet("s3a://dotturin-raw/bikes/")

# GBFS API schema (including Dott's custom fields)
bike_schema = StructType([
  StructField("bike_id", StringType(), True),
  StructField("last_reported", LongType(), True),
  StructField("current_range_meters", IntegerType(), True),
  StructField("current_fuel_percent", DoubleType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lon", DoubleType(), True),
  StructField("is_reserved", BooleanType(), True),
  StructField("is_disabled", BooleanType(), True),
  StructField("vehicle_type_id", StringType(), True),
  StructField("pricing_plan_id", StringType(), True),
  StructField("rental_uris", StructType([
    StructField("android", StringType(), True),
    StructField("ios", StringType(), True)
  ]), True)
])

gbfs_schema = StructType([
  StructField("last_updated", LongType(), True),
  StructField("ttl", IntegerType(), True),
  StructField("version", StringType(), True),
  StructField("data", StructType([
    StructField("bikes", ArrayType(bike_schema), True)
  ]), True)
])

print("[*] Parsing JSON and transforming...")

parsed_df = bronze_df.withColumn("parsed_json", from_json(col("json_payload"), gbfs_schema))

# Explode the bike array as rows into the main table
exploded_df = parsed_df.withColumn("bike", explode(col("parsed_json.data.bikes")))

# Select all the bikes columns, the received timestamp and the last updated timestamp
silver_df = exploded_df.select(
  col("timestamp"), # this is already a timestamp in microseconds, unlike the others which are in seconds
  to_timestamp(from_unixtime(col("parsed_json.last_updated"))).alias("last_updated"),
  col("bike.*")
) \
.withColumn("last_reported", to_timestamp(from_unixtime(col("last_reported"))))

silver_df.printSchema()

print("[*] Writing transformed data in bucket...")

silver_df.coalesce(1).write \
  .mode("overwrite") \
  .parquet("s3a://dotturin-processed/bikes_status/")

print("[+] Transformation completed successfully.")