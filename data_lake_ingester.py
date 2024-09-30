import os
import io
import requests
import boto3
from datetime import datetime
import configparser
import logging

class DataLakeIngester:
  
  def __init__(self,dataset_base_path):
    """ 
    Initialise the DataLakeIngester. 
    :param dataset_base_path: The key prefix to use for this dataset
    """
    # set the logging level and format
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    self.dataset_base_path = dataset_base_path
    self.config = self._load_config()
  
  def ingest_hourly_gharchive(self, process_date: datetime):
    """
    Ingest hourly data from GHArchive and upload to S3.
    """
    # The format of the Hourly json dump files is YYYY-MM-DD-H.json.gz
    # with Hour part without leading zero when single digit
    date_hour = datetime.strftime(process_date, "%Y-%m-%d-%-H")
    data_filename = f"{date_hour}.json.gz"
    data_url = f"http://data.gharchive.org/{data_filename}"
    s3_bucket = self._bronze_bucket_name()
    s3_key = self._generate_sink_key(process_date,data_filename,self.dataset_base_path)
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
  
  def _bronze_bucket_name(self) -> str:
    """ Get S3 bronze bucket name from the config file"""
    try:
      return self.config.get('datalake', 'bronze_bucket')
    except Exception as e:
      logging.error(f"An unexpected error occurred reading bucket name from config.ini file: {e}")
  
  def _generate_sink_key(self, process_date: datetime, filename, sink_base_path) -> str:
    """
    Generate the S3 sink key for the sink file.
    :param process_date: the process date for the current batch.
    :param filename: source filename.
    :param sink_base_path: Key path within the S3 bucket.
    :return: The key portion for the S3 sink path
    """   
    date_partition = datetime.strftime(process_date, "%Y-%m-%d")
    hour_partition = datetime.strftime(process_date, "%H")
    s3_key = f"{sink_base_path}/{date_partition}/{hour_partition}/{filename}"   
    return s3_key 
    
  # Define a callback function to print progress
  def _s3_progress_callback(self, bytes_transferred):
    """
    Callback function to print progress of S3 upload.
    """
    logging.info(f"Transferred: {bytes_transferred} bytes to S3 bucket")
