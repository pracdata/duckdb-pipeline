import os
import io
import requests
import boto3
from datetime import datetime
import configparser
import logging

class DataLakeIngester:
  
  def __init__(self):
    # set the logging level and format
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    self.config = self._load_config()
  
  def ingest_hourly_gharchive(self, s3_bucket, s3_path, process_date):
    """
    Ingest hourly data from GHArchive and upload to S3.
    """
    # The format of the Hourly json dump files is YYYY-MM-DD-H.json.gz
    date_hour = datetime.strftime(process_date, "%Y-%m-%d-%H")
    data_filename = f"{date_hour}.json.gz"
    data_url = f"http://data.gharchive.org/{data_filename}"
    s3_key = f"{s3_path}/{data_filename}"
    data = self.collect_data(data_url)
    self.upload_to_s3(data,s3_bucket,s3_key)
  
  def collect_data(self, data_url):
    """
    Download data from the GHArchive URL.
    """
    logging.info(f"The URL to download is: {data_url}")
    response = requests.get(data_url)
    if response.status_code == 200:
      return io.BytesIO(response.content)
    else:
      logging.error(f"Failed to download file from {data_url}. Status code: {response.status_code}") 
      # This will raise an HTTPError for non-200 status codes
      response.raise_for_status() 

  def upload_to_s3(self, data, bucket, key):
    """
    Upload data to S3.
    """
    s3_client = self._s3_client()
    try:
      s3_client.upload_fileobj(data, bucket, key,Callback=self._s3_progress_callback)
      logging.info(f"Successfully uploaded {key} to {bucket}")
    except boto3.exceptions.S3UploadFailedError as e:
      logging.error(f"Failed to upload {key} to {bucket}: {e}")
      raise
    except Exception as e:
      logging.error(f"An unexpected error occurred uploading to S3: {e}")
      raise   

  def _load_config(self):
    """
    Load configuration from the given path.
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_path)
    return config

  def _s3_client(self):
    """
    Create an S3 client using the loaded credentials.
    """
    return boto3.client('s3',**self._get_s3_credentials())

  def _get_s3_credentials(self):
    """
    Retrieve S3 credentials from the configuration.
    """
    # the keys must be the same key names the boto3.client expects
    credentials = {
      "aws_access_key_id": self.config.get('aws', 's3_access_key_id'),
      "aws_secret_access_key": self.config.get('aws', 's3_secret_access_key')
    }
    # check if options are present in the config file and add them
    if self.config.has_option('aws', 's3_region_name'):
      credentials["region_name"] = self.config.get('aws', 's3_region_name')
    if self.config.has_option('aws', 's3_endpoint_url'):
      credentials["endpoint_url"] = self.config.get('aws', 's3_endpoint_url')
    return credentials
  
  # Define a callback function to print progress
  def _s3_progress_callback(self, bytes_transferred):
    """
    Callback function to print progress of S3 upload.
    """
    logging.info(f"Transferred: {bytes_transferred} bytes to S3 bucket")
