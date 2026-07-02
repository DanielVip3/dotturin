from common import *
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
  dag_id='turin_dott_transformation',
  default_args=default_args,
  description='Batch transformation (bronze to silver and silver to gold) every 30 minutes',
  schedule='*/30 * * * *',
  start_date=start_date,
  catchup=False
)
def turin_dott_transformation_dag():
  run_silver = SparkSubmitOperator(
    task_id='run_silver',
    application='/opt/airflow/scripts/transform_silver.py',
    conn_id='spark_default',
    conf={
      'spark.cores.max': SPARK_APP_CORES
    },
    verbose=True
  )

  run_gold = SparkSubmitOperator(
    task_id='run_gold',
    application='/opt/airflow/scripts/transform_gold.py',
    conn_id='spark_default',
    conf={
      'spark.cores.max': SPARK_APP_CORES
    },
    verbose=True
  )

  run_silver >> run_gold

turin_dott_transformation_dag()