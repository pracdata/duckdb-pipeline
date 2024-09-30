import pytest
import duckdb
from data_lake_transformer import DataLakeTransformer
import pandas as pd

@pytest.fixture
def dl_transformer():
  return DataLakeTransformer()

@pytest.fixture
def mock_duckdb_connection():
  con = duckdb.connect(':memory:')
  yield con
  con.close()

@pytest.fixture
def sample_raw_data(mock_duckdb_connection):
  mock_duckdb_connection.execute("""
      CREATE TABLE gharchive_raw AS SELECT * FROM (
          VALUES
          (1, {'id': 101, 'login': 'user1', 'display_login': 'User One'}, 'PushEvent', {'id': 201, 'name': 'repo1', 'url': 'https://github.com/user1/repo1'}, '2023-01-01 12:00:00'),
          (2, {'id': 102, 'login': 'user2', 'display_login': 'User Two'}, 'IssuesEvent', {'id': 202, 'name': 'repo2', 'url': 'https://github.com/user2/repo2'}, '2023-01-02 13:00:00')
      ) AS t(id, actor, type, repo, created_at)
  """)
  return "gharchive_raw"

@pytest.fixture
def mock_s3_bronze_parquet_data(mock_duckdb_connection):
    # Create a mock table that simulates data from multiple Parquet files
    mock_duckdb_connection.execute("""
        CREATE TABLE mock_bronze_data AS SELECT * FROM (
            VALUES
            ('PushEvent', 1, 'repo1', 'http://repo1.com', '2023-01-01 10:00:00'),
            ('PushEvent', 1, 'repo1', 'http://repo1.com', '2023-01-01 11:00:00'),
            ('IssueEvent', 2, 'repo2', 'http://repo2.com', '2023-01-01 12:00:00'),
            ('PushEvent', 1, 'repo1', 'http://repo1.com', '2023-01-02 10:00:00')
        ) AS t(event_type, repo_id, repo_name, repo_url, event_date)
    """)
    return "mock_bronze_data"

def test_clean_raw_gharchive(mock_duckdb_connection, sample_raw_data, monkeypatch):
  # Create an instance of DataLakeTransformer
  transformer = DataLakeTransformer()
  # Monkeypatch the duckdb connection to use our mock connection
  monkeypatch.setattr(transformer, 'con', mock_duckdb_connection)
  # Call the method we're testing
  result = transformer.clean_raw_gharchive(sample_raw_data)
  # Assert that the result is a DuckDB table
  assert isinstance(result, duckdb.DuckDBPyRelation)
  # Convert the result to a pandas DataFrame for easier assertions
  df = result.to_df()
  # Assert the correct number of rows
  assert len(df) == 2
  # Assert the correct column names
  expected_columns = ['event_id', 'user_id', 'user_name', 'user_display_name', 'event_type', 'repo_id', 'repo_name', 'repo_url', 'event_date']
  assert list(df.columns) == expected_columns
  # Assert some specific values
  assert df.loc[0, 'event_id'] == 1
  assert df.loc[0, 'user_id'] == 101
  assert df.loc[0, 'user_name'] == 'user1'
  assert df.loc[0, 'user_display_name'] == 'User One'
  assert df.loc[0, 'event_type'] == 'PushEvent'
  assert df.loc[0, 'repo_id'] == 201
  assert df.loc[0, 'repo_name'] == 'repo1'
  assert df.loc[0, 'repo_url'] == 'https://github.com/user1/repo1'
  assert df.loc[0, 'event_date'] == '2023-01-01 12:00:00'
  # Assert that the gharchive_clean table was created in the connection
  clean_table = mock_duckdb_connection.table("gharchive_clean")
  assert isinstance(clean_table, duckdb.DuckDBPyRelation)

def test_extract_filename_from_s3_path(dl_transformer):
  # Test case 1: S3 path with extension, keep extension
  s3_path1 = 's3://my-bucket/path/to/file.txt'
  assert dl_transformer._extract_filename_from_s3_path(s3_path1) == 'file.txt'

  # Test case 2: S3 path with extension, remove extension
  assert dl_transformer._extract_filename_from_s3_path(s3_path1, remove_extension=True) == 'file'

  # Test case 3: S3 path without extension
  s3_path2 = 's3://my-bucket/path/to/file'
  assert dl_transformer._extract_filename_from_s3_path(s3_path2) == 'file'
  assert dl_transformer._extract_filename_from_s3_path(s3_path2, remove_extension=True) == 'file'

  # Test case 4: S3 path with multiple dots in filename
  s3_path3 = 's3://my-bucket/path/to/file.with.dots.txt'
  assert dl_transformer._extract_filename_from_s3_path(s3_path3) == 'file.with.dots.txt'
  assert dl_transformer._extract_filename_from_s3_path(s3_path3, remove_extension=True) == 'file.with.dots'

  # Test case 5: S3 path without 's3://' prefix
  s3_path4 = 'my-bucket/path/to/file.txt'
  assert dl_transformer._extract_filename_from_s3_path(s3_path4) == 'file.txt'
  assert dl_transformer._extract_filename_from_s3_path(s3_path4, remove_extension=True) == 'file'


def test_aggregate_raw_gharchive(mock_duckdb_connection, mock_s3_bronze_parquet_data, monkeypatch):
  # Create an instance of DataLakeTransformer
  transformer = DataLakeTransformer()
  # Monkeypatch the duckdb connection to use our mock connection
  monkeypatch.setattr(transformer, 'con', mock_duckdb_connection)
  # Call the method to aggregate the data
  result = transformer.aggregate_raw_gharchive(mock_s3_bronze_parquet_data)
  # Convert the result to a pandas DataFrame for easier assertion
  df = result.to_df()
  # Assert the shape of the result
  assert df.shape == (3, 6)
  # Assert the aggregated values
  expected_data = [
      ('PushEvent', 1, 'repo1', 'http://repo1.com', '2023-01-01', 2),
      ('IssueEvent', 2, 'repo2', 'http://repo2.com', '2023-01-01', 1),
      ('PushEvent', 1, 'repo1', 'http://repo1.com', '2023-01-02', 1)
  ]
  for row in expected_data:
      assert row in [tuple(r) for r in df.itertuples(index=False)]
  # Clean up
  transformer.con.close()
