from common import SPARK_APP_CORES, SPARK_APP_MEMORY, SPARK_NUM_PARTITIONS, start_date, default_args
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
  dag_id="gold_hourly_transformation",
  default_args=default_args,
  description="Incremental batch transformation from silver to gold every hour, to populate the data warehouse",
  schedule="0 * * * *",
  start_date=start_date,
  catchup=False,
)
def gold_hourly_transformation():
  # Validate the silver Delta Lake input data (to prevent writing incorrect data to gold)
  # We use a Spark job as it has to read from Delta Lake
  validate_silver = SparkSubmitOperator(
    task_id="validate_silver",
    application="/opt/airflow/scripts/silver/validate.py",
    py_files="/opt/airflow/scripts/common.py",
    conn_id="spark_default",
    conf={
      "spark.cores.max": SPARK_APP_CORES,
      "spark.driver.memory": SPARK_APP_MEMORY,
      "spark.executor.memory": SPARK_APP_MEMORY,
      "spark.sql.shuffle.partitions": SPARK_NUM_PARTITIONS,
    },
    verbose=True,
  )

  # Run Spark job to populate ClickHouse data warehouse
  run_gold = SparkSubmitOperator(
    task_id="run_gold",
    application="/opt/airflow/scripts/gold/clickhouse_hourly.py",
    py_files="/opt/airflow/scripts/common.py",
    conn_id="spark_default",
    conf={
      "spark.cores.max": SPARK_APP_CORES,
      "spark.driver.memory": SPARK_APP_MEMORY,
      "spark.executor.memory": SPARK_APP_MEMORY,
      "spark.sql.shuffle.partitions": SPARK_NUM_PARTITIONS,
    },
    verbose=True,
  )

  # Validate the gold data
  validate_gold = BashOperator(
    task_id="validate_gold",
    bash_command="soda scan -d clickhouse -c /opt/soda/configuration.yml /opt/soda/checks_gold.yml",
  )

  validate_silver >> run_gold >> validate_gold


gold_hourly_transformation()
