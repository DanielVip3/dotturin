from common import start_date, default_args
from airflow.sdk import dag, task


@dag(
  dag_id="twitch_api_ingestion",
  default_args=default_args,
  description="1 minutes ingestion flow for Twitch API",
  schedule="*/1 * * * *",
  start_date=start_date,
  catchup=False,
)
def twitch_api_ingestion_dag():
  @task(task_id="run_kafka_producer")
  def trigger_producer():
    import subprocess
    import sys

    subprocess.run([sys.executable, "-u", "/opt/airflow/scripts/producer.py"], check=True)

  # Run the producer Python script
  trigger_producer()


twitch_api_ingestion_dag()
