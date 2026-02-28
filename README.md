# Data integration pipeline


1. Data lands into a raw zone (s3/bronze/) 
2. Data procesing (with some feature extraction), validation, and canonical modelling (pydantic) (s3/silver/processed)
3. Silver data loading into "silver" data vault
4. Data auditing of samples of processed data (great expectations) (s3/silver/audits)
5. Data deduplication and matching (kanoniv) where we store entity resolution results (s3/silver/integrated)
5. Data loading into gold data vault (pyarrow + deltatables) using data vault 2.0 architecture (s3/gold/)
6. Data aggregation for data marts (s3/gold/marts)



# TODO
- finishn integrated record
- load integrated records into integrated_records.db
- create id bridge
- create master view
- add airflow?
- readme


# Setup 

Setup can be done via make commands:

- Start necessary Docker containers (Minio and Postgres) with:
```
make infra_up
```

- Stop Docker containers (Minio and Postgres) with:
```
make infra_down
```

- Check Docker containers status (Minio and Postgres) with:
```
make infra_status
```


# Execution workflow

# TODO I still need to create the make commands

1. Upload test data to S3 bucket `data` in `bronze` area:

```
make upload_bronze
# or
python src/data_integration_pipeline/jobs/1_upload_bronze.py
```

2. Process bronze data S3 bucket `data` and writes to S3 bucket `data` in `silver` folder. Note that data is read and written into S3 directly without landing into a local folder. Depending on the requirements and workflow bottleneck, this could change, and we could first download to local, e.g., if we want to distribute a file into batches of data and process those in parallel. As with everything, it depends on the business context.

```
make process_bronze_to_silver
# or
python src/data_integration_pipeline/jobs/2_process_bronze_to_silver.py
```