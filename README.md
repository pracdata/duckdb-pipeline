# Using DuckDB for building data pipelines
Build a simple data lake based on Medallion Archirecture, to incrementally ingest and process Github events.

![DuckDB gharchive pipeline](https://github.com/user-attachments/assets/0b37081e-a786-4687-9a3f-99c768f963b1)


### Clone The Repository 
```bash
$ git clone https://github.com/pracdata/duckdb-pipeline.git
```

### Setup Python Virtual Environment
```bash
$ cd duckdb-pipeline
$ python3 -m venv .venv
$ source .venv/bin/activate
# Install required packages
$ pip install -r requirements.txt
```

### Configuration

1. Rename `config.ini.template` to `config.ini`
2. Edit `config.ini` and fill in your actual AWS S3 credential values in the `[aws]` section.
3. If you are using a S3 compatible storage, setup the `s3_endpoint_url` parameter as well. Otherwise remove the line
4. Edit `config.ini` and fill in the bucket names in `[datalake]` section for each zone in your data lake.

### Scheduling

There are Python scripts available in `scripts` directory for each phase (Ingestion, Serilisation, Aggregation) for calling with a scheduler like **Crontab**.
Following shows sample cron statements to run each pipeline script at an appropriate time. 
Update the paths to match your setting and also ensure you allow enough time between each pipeline to complete. 

```
# schedule the ingestion pipeline script to run 15 minutes past each hour
15 * * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_ingest_source_data.py >> /tmp/ingest_source_data.out 2>&1
# schedule the serialisation pipeline script to run 30 minutes past each hour
30 * * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_serialise_raw_data.py >> /tmp/serialise_raw_data.out 2>&1
# schedule the aggregation pipeline script to run 2 hours past midnight
0 2 * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_agg_silver_data.py >> /tmp/aggregate_silver_data.out 2>&1
```
