from common import get_spark_session

# Initialize Spark session
spark = get_spark_session("DotTurinStreamingConsumer", master="spark://spark-master:7077")

# Flow from Kafka to Spark
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", "dotturin-bike-status") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

readable_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp")

print("[*] Setup complete. Waiting for data...")

# Write stream in Delta Lake format in bucket 'dotturin-raw'
query = readable_df.writeStream \
  .format("delta") \
  .option("path", "s3a://dotturin-raw/bikes/") \
  .option("checkpointLocation", "s3a://dotturin-raw/checkpoints/") \
  .outputMode("append") \
  .start()

query.awaitTermination()