from common import SPARK_APP_CORES, start_date, default_args
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
  dag_id='gold_hourly_transformation',
  default_args=default_args,
  description='Incremental batch transformation from silver to gold every hour, to populate the data warehouse',
  schedule='0 * * * *',
  start_date=start_date,
  catchup=False
)
def gold_hourly_transformation():
  SparkSubmitOperator(
    task_id='run_gold',
    application='/opt/airflow/scripts/gold/clickhouse_hourly.py',
    conn_id='spark_default',
    conf={
      'spark.cores.max': SPARK_APP_CORES
    },
    verbose=True
  )

gold_hourly_transformation()