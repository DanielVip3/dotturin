import sys
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def main():
  if len(sys.argv) < 2:
    print("Usage: optimize_delta.py [bronze|silver] [year] [month] [day]")
    sys.exit(1)

  layer = sys.argv[1]

  # Initialize Spark session
  spark = SparkSession.builder \
    .appName(f"DotTurinOptimize-{layer}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true") \
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  if layer == "bronze":
    print("[*] Optimizing bronze layer...")
    
    # 128 MB = 134.217.728 bytes
    spark.sql("ALTER TABLE delta.`s3a://dotturin-raw/bikes/` SET TBLPROPERTIES ('delta.targetFileSize' = '134217728')")
    
    spark.sql("OPTIMIZE delta.`s3a://dotturin-raw/bikes/`")
    print("[+] Bronze optimized successfully.")

  elif layer == "silver":
    if len(sys.argv) != 5:
      print("[!] Error: year, month and day required.")
      sys.exit(1)
        
    year, month, day = sys.argv[2], sys.argv[3], sys.argv[4]
    
    print(f"[*] Optimizing silver layer on date Year={year} Month={month} Day={day}...")

    # 512 MB = 536.870.912 bytes
    spark.sql("ALTER TABLE delta.`s3a://dotturin-processed/bikes_status/` SET TBLPROPERTIES ('delta.targetFileSize' = '536870912')")
    
    spark.sql(f"OPTIMIZE delta.`s3a://dotturin-processed/bikes_status/` WHERE year = {year} AND month = {month} AND day = {day}")
    print(f"[+] Silver optimized successfully for Year={year} Month={month} Day={day}.")

  spark.stop()

if __name__ == "__main__":
  main()