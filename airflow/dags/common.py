import pendulum
from datetime import timedelta
import os

SPARK_APP_CORES = os.environ.get("SPARK_APP_CORES", "1")
SPARK_APP_MEMORY = os.environ.get("SPARK_APP_MEMORY", "1g")
SPARK_NUM_PARTITIONS = os.environ.get("SPARK_NUM_PARTITIONS", "2")

default_args = {
  "owner": "root",
  "depends_on_past": False,
  "retries": 1,
  "retry_delay": timedelta(seconds=30),
}

start_date = pendulum.datetime(2026, 6, 30, tz="UTC")
