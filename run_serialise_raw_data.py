#!/usr/bin/env python3
from data_lake_transformer import DataLakeTransformer
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
  try:
    transformer = DataLakeTransformer(dataset_base_path='gharchive/events')
    now = datetime.utcnow()
    # Calculate the process_date (2 hours before to ensure ingestion is run)
    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    transformer.serialise_raw_data(process_date)
    logging.info(f"Successfully serialised raw data for {process_date}")
  except Exception as e:
    logging.error(f"Error in serialise_raw_data: {str(e)}")

if __name__ == "__main__":
  main()
