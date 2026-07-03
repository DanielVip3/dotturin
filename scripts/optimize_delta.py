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
  spark = get_spark_session(f"DotTurinOptimize-{layer}")

  if layer == "bronze":
    if len(sys.argv) != 5:
      print("[!] Error: year, month and day required.")
      sys.exit(1)
        
    year, month, day = sys.argv[2], sys.argv[3], sys.argv[4]
    
    print(f"[*] Optimizing bronze layer on date Year={year} Month={month} Day={day}...")

    # 512 MB = 536.870.912 bytes
    spark.sql("ALTER TABLE delta.`s3a://dotturin-raw/bikes/` SET TBLPROPERTIES ('delta.targetFileSize' = '536870912')")
    
    spark.sql(f"OPTIMIZE delta.`s3a://dotturin-raw/bikes/` WHERE year = {year} AND month = {month} AND day = {day}")
    print(f"[+] Bronze optimized successfully for Year={year} Month={month} Day={day}.")

  elif layer == "silver":    
    print(f"[*] Optimizing silver layer...")

    # 512 MB = 536.870.912 bytes
    spark.sql("ALTER TABLE delta.`s3a://dotturin-processed/bikes_status/` SET TBLPROPERTIES ('delta.targetFileSize' = '536870912')")
    
    spark.sql(f"OPTIMIZE delta.`s3a://dotturin-processed/trips/`")
    print(f"[+] Silver optimized successfully.")

  spark.stop()

if __name__ == "__main__":
  main()