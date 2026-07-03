from common import *
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
  dag_id='turin_dott_transformation',
  default_args=default_args,
  description='Batch transformation (silver to gold) every 30 minutes',
  schedule='*/30 * * * *',
  start_date=start_date,
  catchup=False
)
def turin_dott_transformation_dag():
  # Temporarily disabled as gold is not up-to-date.
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

turin_dott_transformation_dag()