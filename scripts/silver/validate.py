from common import get_spark_session
from soda.scan import Scan


def main():
  spark = get_spark_session("TwitchySodaSilverValidation")

  streams_df = spark.read.format("delta").load("s3a://twitch-silver/streams/")
  tags_df = spark.read.format("delta").load("s3a://twitch-silver/stream_tags/")
  transitions_df = spark.read.format("delta").load("s3a://twitch-silver/stream_transitions/")
  games_df = spark.read.format("delta").load("s3a://twitch-silver/games/")

  # Register tables as temporary views so Soda can query them
  streams_df.createOrReplaceTempView("streams")
  tags_df.createOrReplaceTempView("stream_tags")
  transitions_df.createOrReplaceTempView("stream_transitions")
  games_df.createOrReplaceTempView("games")

  # Initialize Soda scan
  scan = Scan()
  scan.set_verbose(True)
  scan.set_scan_definition_name("Silver validation")
  scan.set_data_source_name("spark_df")

  # Attach Spark session and YAML file
  scan.add_spark_session(spark)
  scan.add_sodacl_yaml_file("/opt/soda/checks_silver.yml")

  scan.execute()
  print(scan.get_logs_text())

  # Fail the job if data quality is compromised
  if scan.has_error_or_warning_logs():
    raise Exception(f"Data quality checks failed for silver layer! Message: {scan.get_error_or_warning_logs_text()}")


if __name__ == "__main__":
  main()
