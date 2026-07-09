from common import (
  SPARK_APP_CORES,
  SPARK_APP_MEMORY,
  SPARK_NUM_PARTITIONS,
  start_date,
  default_args,
)
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
  dag_id="nightly_maintenance",
  default_args=default_args,
  description="Nightly Delta Lake optimization for bronze and silver layers data at 00:00",
  schedule="0 0 * * *",
  start_date=start_date,
  catchup=False,
)
def nightly_maintenance_dag():
  # Task 1 for bronze on current year and month
  # data_interval_start inside the template refers to the previous day
  # (i.e. the day that just passed at 00:00).
  optimize_bronze = SparkSubmitOperator(
    task_id="optimize_bronze",
    application="/opt/airflow/scripts/optimize_delta.py",
    application_args=[
      "bronze",
      '{{ data_interval_start.strftime("%Y") }}',
      '{{ data_interval_start.strftime("%-m") }}',
    ],
    conn_id="spark_default",
    conf={
      "spark.cores.max": SPARK_APP_CORES,
      "spark.driver.memory": SPARK_APP_MEMORY,
      "spark.executor.memory": SPARK_APP_MEMORY,
      "spark.sql.shuffle.partitions": SPARK_NUM_PARTITIONS,
    },
    verbose=True,
  )

  # Task 2 for silver
  optimize_silver = SparkSubmitOperator(
    task_id="optimize_silver",
    application="/opt/airflow/scripts/optimize_delta.py",
    application_args=["silver"],
    conn_id="spark_default",
    conf={
      "spark.cores.max": SPARK_APP_CORES,
      "spark.driver.memory": SPARK_APP_MEMORY,
      "spark.executor.memory": SPARK_APP_MEMORY,
      "spark.sql.shuffle.partitions": SPARK_NUM_PARTITIONS,
    },
    verbose=True,
  )

  # Run the two tasks sequentially
  optimize_bronze >> optimize_silver


nightly_maintenance_dag()
