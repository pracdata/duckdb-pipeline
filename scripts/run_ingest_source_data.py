#!/usr/bin/env python3
import sys
import os
import logging
from datetime import datetime, timedelta
# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_lake_ingester import DataLakeIngester

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
  try:
    ingester = DataLakeIngester("gharchive/events")
    now = datetime.utcnow()
    # Calculate the process_date (1 hour before to ensure data availability at source)
    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    ingester.ingest_hourly_gharchive(process_date)
    logging.info(f"Successfully ingested data for {process_date}")
  except Exception as e:
    logging.error(f"Error in ingest_hourly_gharchive: {str(e)}")

if __name__ == "__main__":
  main()
