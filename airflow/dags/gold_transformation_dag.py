from common import *
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# TODO: REWRITE ENTIRELY

@dag(
  dag_id='gold_transformation',
  default_args=default_args,
  description='Batch transformation from silver to gold every 30 minutes',
  schedule='*/30 * * * *',
  start_date=start_date,
  catchup=False
)
def gold_transformation_dag():
  # Temporarily disabled as there is no gold.
  #run_gold = SparkSubmitOperator(
  #  task_id='run_gold',
  #  application='/opt/airflow/scripts/transform_gold.py',
  #  conn_id='spark_default',
  #  conf={
  #    'spark.cores.max': SPARK_APP_CORES
  #  },
  #  verbose=True
  #)
  pass

gold_transformation_dag()