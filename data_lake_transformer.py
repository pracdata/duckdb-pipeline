import duckdb
import configparser
from datetime import datetime
import logging
import os
import uuid
from datetime import datetime

class DataLakeTransformer:
  """
  A class for transforming and moving data between different stages of a data lake.
  """  
  def __init__(self,dataset_base_path):
    """ 
    Initialise the DataLakeTransformer. 
    
    :param dataset_base_path: The key prefix to use for this dataset
    """
    # set the logging level and format
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    self.dataset_base_path = dataset_base_path
    self.config = self._load_config()
    self.con = self.duckdb_connection()
    self._set_duckdb_s3_credentials()
    logging.info("DuckDB connection initiated")
  
  def duckdb_connection(self) -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB connection."""
    con = duckdb.connect()
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    return con

  def serialise_raw_data(self, process_date: datetime) -> None:
    """
    Serialize and clean raw data, then export to parquet format on next zone.
    
    :param process_date: the process date corresponding to the hourly partition to serialise
    """
    try:
      source_bucket = self._datalake_bucket_name()['bronze']
      sink_bucket = self._datalake_bucket_name()['silver']
      source_path = self._raw_hourly_file_path(source_bucket, self.dataset_base_path, process_date)
      gharchive_raw_result = self.register_raw_gharchive(source_path)
      gharchive_clean_result = self.clean_raw_gharchive(gharchive_raw_result.alias)
      sink_path = self._create_sink_path('clean', sink_bucket, self.dataset_base_path, process_date, True)
      logging.info(f"DuckDB - serialise and export cleaned data to {sink_path}")
      gharchive_clean_result.write_parquet(sink_path)
    except Exception as e:
      logging.error(f"Error in serialise_raw_data: {str(e)}")
      raise
  
  def aggregate_silver_data(self, process_date: datetime) -> None:
    """
    Aggregate raw data and export to parquet format.
    
    :param process_date: the process date corresponding to the daily partition to aggregate
    """
    try:
      source_bucket = self._datalake_bucket_name()['silver']
      sink_bucket = self._datalake_bucket_name()['gold']
      source_path = self._silver_daily_file_path(source_bucket, self.dataset_base_path, process_date)
      logging.info(f"DuckDB - aggregate silver data in {source_path}")
      gharchive_agg_result = self.aggregate_raw_gharchive(source_path)
      sink_path = self._create_sink_path('agg', sink_bucket, self.dataset_base_path, process_date)
      logging.info(f"DuckDB - export aggregated data to {sink_path}")
      gharchive_agg_result.write_parquet(sink_path)   
    except Exception as e:
      logging.error(f"Error in aggregate_silver_data: {str(e)}")
      raise

  def register_raw_gharchive(self, source_path) -> duckdb.DuckDBPyRelation:
    """
    Create a an in-memory table from raw GHArchive source data
    
    :param source_path: Full Path to the source data on lake.
    :return: DuckDB result object representing the raw table.
    """
    logging.info(f"DuckDB - collect source data files: {source_path}")
    self.con.execute(f"CREATE OR REPLACE TABLE gharchive_raw \
                      AS FROM read_json_auto('{source_path}', ignore_errors=true)")
    return self.con.table("gharchive_raw")
  
  def clean_raw_gharchive(self,raw_dataset) -> duckdb.DuckDBPyRelation:
    """
    Clean the raw GHArchive data and only selected attributed we are interest in.
    
    :param raw_dataset: Name of the DuckDB raw dataset table.
    :return: DuckDB result object representing the cleaned table.
    """
    query = f'''
      SELECT 
        id AS "event_id",
        actor.id AS "user_id",
        actor.login AS "user_name",
        actor.display_login AS "user_display_name",
        type AS "event_type",
        repo.id AS "repo_id",
        repo.name AS "repo_name",
        repo.url AS "repo_url",
        created_at AS "event_date"
      FROM '{raw_dataset}'
    '''
    logging.info("DuckDB - clean data")
    self.con.execute(f"CREATE OR REPLACE TABLE gharchive_clean AS FROM ({query})")
    return self.con.table("gharchive_clean")

  def aggregate_raw_gharchive(self, raw_dataset) -> duckdb.DuckDBPyRelation:
    """
    Aggregate the raw GHArchive data.
    
    :param raw_dataset: Full Path to the raw dataset on data lake.
    :return: DuckDB result object representing the aggregated table.
    """
    query = f'''
      SELECT 
        event_type,
        repo_id,
        repo_name,
        repo_url,
        DATE_TRUNC('day',CAST(event_date AS TIMESTAMP)) AS event_date,
        count(*) AS event_count
      FROM '{raw_dataset}'
      GROUP BY ALL
    '''
    self.con.execute(f"CREATE OR REPLACE TABLE gharchive_agg AS FROM ({query})")
    return self.con.table("gharchive_agg")

  def _create_sink_path(self, data_type, sink_bucket, sink_base_path, process_date: datetime, has_hourly_partition: bool = False) -> str:
    """
    Create the full S3 path for the sink file.
    
    :param sink_bucket: S3 bucket for the output.
    :param sink_base_path: Key path within the S3 bucket.
    :param data_type: Type of data being processed.
    :param process_date: the process date corresponding to the hourly partition
    :return: Full S3 path for the sink file.
    """
    partitions_path = self._partition_path(process_date,has_hourly_partition)
    sink_filename = self._generate_export_filename(data_type,process_date,has_hourly_partition)
    return f"s3://{sink_bucket}/{sink_base_path}/{partitions_path}/{sink_filename}"
  
  def _extract_filename_from_s3_path(self, s3_path, remove_extension=False) -> str:
    """
    Extract filename from S3 path, optionally removing the extension.
    
    :param s3_path: S3 path to extract filename from.
    :param remove_extension: Whether to remove the file extension.
    :return: Extracted filename.
    """
    path_without_prefix = s3_path.replace('s3://', '')
    full_filename = os.path.basename(path_without_prefix)
    if remove_extension:
      # List of common compression extensions
      compression_extensions = ['.gz', '.bz2', '.zip', '.xz', '.zst']
      # Split the filename and extension
      filename, extension = os.path.splitext(full_filename)
      # Check if the extension is a compression format
      if extension.lower() in compression_extensions:
        filename, _ = os.path.splitext(filename)
      return filename
    else:
      return full_filename
  
  def _raw_hourly_file_path(self, source_bucket, source_base_path, process_date: datetime) -> str:
    """Generate the S3 path for hourly silver exported files."""
    partitions_path = self._partition_path(process_date,True)
    s3_key = f"s3://{source_bucket}/{source_base_path}/{partitions_path}/*"   
    return s3_key 

  def _silver_daily_file_path(self, source_bucket, source_base_path, process_date: datetime) -> str:
    """Get the S3 path for silver files on the day level partition."""
    partitions_path = self._partition_path(process_date,False)
    s3_key = f"s3://{source_bucket}/{source_base_path}/{partitions_path}/*/*.parquet"   
    return s3_key 
  
  def _partition_path(self,process_date: datetime, has_hourly_partition: bool = False):
    """Generate the partition path based on the process date."""
    if has_hourly_partition:
        partition_path = process_date.strftime("%Y-%m-%d/%H")
    else:
        partition_path = process_date.strftime("%Y-%m-%d")
    return partition_path
  
  def _generate_export_filename(self, data_type, process_date: datetime, has_hourly_partition: bool = False, file_extension='parquet'):
    """Generate a filename for the exported data file"""
    if has_hourly_partition:
        timestamp = process_date.strftime("%Y%m%d_%H")
    else:
        timestamp = process_date.strftime("%Y%m%d")
    return f"{data_type}_{timestamp}.{file_extension}"
   
  # def _generate_export_filename(self, data_type, file_extension='parquet', partition_key=None) -> str:
  #   """
  #   Generate a unique filename for the exported data file.

  #   :param base_name: Base name for the file.
  #   :param data_type: Type of data being processed.
  #   :return: Generated filename.
  #   """
  #   timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
  #   unique_id = str(uuid.uuid4())[:8]  # Use first 8 characters of a UUID
  #   filename_parts = [data_type, timestamp, unique_id]
  #   if partition_key:
  #     filename_parts.insert(2, partition_key)
  #   return "_".join(filename_parts) + "." + file_extension
  
  def _load_config(self):
    """ Load configuration from the given path """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_path)
    return config

  def _datalake_bucket_name(self) -> dict:
    """ Get S3 bucket names from the config file """
    try:
      buckets = {}
      buckets['bronze'] = self.config.get('datalake', 'bronze_bucket')
      buckets['silver'] = self.config.get('datalake', 'silver_bucket')
      buckets['gold'] = self.config.get('datalake', 'gold_bucket')
      return buckets
    except Exception as e:
      logging.error(f"An unexpected error occurred reading bucket names from config.ini file: {e}")
          
  def _set_duckdb_s3_credentials(self) -> None:
    """ Read S3 credentials and endpoint from config file """
    aws_access_key_id = self.config.get('aws', 's3_access_key_id')
    aws_secret_access_key = self.config.get('aws', 's3_secret_access_key')
    s3_endpoint = self.config.get('aws', 's3_endpoint', fallback=None)
    # Set S3 credentials
    self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
    self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
    # Set S3 endpoint if provided
    if s3_endpoint:
      self.con.execute(f"SET s3_endpoint='{s3_endpoint}'")

  def __del__(self):
    """Ensure the DuckDB connection is closed when the object is destroyed."""
    if hasattr(self, 'con'):
      self.con.close()
