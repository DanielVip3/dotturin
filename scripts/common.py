import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def get_spark_session(app_name: str, master: str|None = None) -> SparkSession:
  """
  Initialize and return a SparkSession pre-configured for Delta Lake and MinIO.
  """

  spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
    .config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
  
  if master is not None:
    spark = spark.master(master)
  
  spark = spark.getOrCreate()
      
  spark.sparkContext.setLogLevel("WARN")
  return spark