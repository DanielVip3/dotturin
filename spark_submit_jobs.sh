#!/bin/bash
set -e

SPARK_MASTER="spark://spark-master:7077"
JOBS_DIR="/app/jobs"

echo "[*] Starting bronze ingestion..."
/opt/spark/bin/spark-submit --master $SPARK_MASTER --conf spark.cores.max=${SPARK_APP_CORES} $JOBS_DIR/consumer_bronze.py &
BRONZE_PID=$!

echo "[*] Starting silver transformations..."
# If spark-submit fails (crashes), sleep for 185s before restarting silver in loop via 'until'.
# It gives the time to the bronze layer to write his first table in the data lake
( until /opt/spark/bin/spark-submit --master $SPARK_MASTER --conf spark.cores.max=${SPARK_APP_CORES} $JOBS_DIR/transform_silver.py; do sleep 185; done ) &
SILVER_PID=$!

# If the bronze job dies, the container exits
wait $BRONZE_PID
echo "[-] Bronze job terminated!"
exit 1