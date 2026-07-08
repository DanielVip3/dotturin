from common import get_spark_session
import sys
from dotenv import load_dotenv

load_dotenv()

def main():
  if len(sys.argv) < 2:
    print("Usage: optimize_delta.py [bronze|silver] [year] [month] [day]")
    sys.exit(1)

  layer = sys.argv[1]

  # Initialize Spark session
  spark = get_spark_session(f"DeltaLakeOptimize-{layer}")

  if layer == "bronze":
    if len(sys.argv) < 4:
      print("[!] Error: At least year and month are required for bronze optimization.")
      sys.exit(1)

    spark.sql(f"OPTIMIZE delta.`s3a://twitch-bronze/games/`")

    year, month = int(sys.argv[2]), int(sys.argv[3])

    if len(sys.argv) == 4:
      print(f"[*] Optimizing bronze layer for Year={year}, Month={month}...")
      spark.sql(f"OPTIMIZE delta.`s3a://twitch-bronze/streams/` WHERE year = {year} AND month = {month}")
      print(f"[+] Bronze optimized successfully for Year={year}, Month={month}.")
    elif len(sys.argv) == 5:
      day = int(sys.argv[4])

      print(f"[*] Optimizing bronze layer for Year={year}, Month={month}, Day={day}...")
      spark.sql(f"OPTIMIZE delta.`s3a://twitch-bronze/streams/` WHERE year = {year} AND month = {month} AND day = {day}")
      print(f"[+] Bronze optimized successfully for Year={year}, Month={month}, Day={day}.")

  elif layer == "silver":    
    print(f"[*] Optimizing silver layer...")

    spark.sql(f"OPTIMIZE delta.`s3a://twitch-silver/streams/`")
    spark.sql(f"OPTIMIZE delta.`s3a://twitch-silver/stream_tags/`")
    spark.sql(f"OPTIMIZE delta.`s3a://twitch-silver/stream_transitions/`")
    spark.sql(f"OPTIMIZE delta.`s3a://twitch-silver/games/`")

    print(f"[+] Silver optimized successfully.")

  spark.stop()

if __name__ == "__main__":
  main()