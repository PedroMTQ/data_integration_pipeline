# Data integration pipeline


1. Data lands into a raw zone (s3/bronze/) 
2. Data procesing (with some feature extraction), validation, and canonical modelling (pydantic) (s3/silver/processed)
3. Silver data loading into "silver" data vault
4. Data auditing of samples of processed data (great expectations) (s3/silver/audits)
5. Data deduplication and matching (kanoniv) where we store entity resolution results (s3/silver/integrated)
5. Data loading into gold data vault (pyarrow + deltatables) using data vault 2.0 architecture (s3/gold/)
6. Data aggregation for data marts (s3/gold/marts)




# TODO





### Phase 1: Environment & Foundation (Day 1 - AM)

* [x] **Set up Local Infrastructure:** Use Docker to spin up a local Airflow instance and a local S3 emulator (like LocalStack) or just use local file directories mapped as S3 paths.
* [x] **Sample Data Creation:** Generate 3 "messy" CSV files to simulate different registries:
* `city_registry.csv` (Headers: `Firm_Name`, `Addr`, `Cert_Status`)
* `state_registry.csv` (Headers: `Company`, `Location`, `License_Expiry`)
* `federal_uei.csv` (Headers: `Legal_Entity_Name`, `UEI_Number`, `Physical_Address`)


* [ ] **Project Structure:** Create your folders: `dags/`, `src/converters/`, `src/validation/`, `src/er_framework/`, and `config/`.

### Phase 2: The "Silver" Validation Layer (Day 1 - PM)

* [x] **Source Mapping Config:** Create a `sources.yaml` that maps the messy headers from your 3 samples to your canonical field names (e.g., `Firm_Name` -> `normalized_company_name`).
* [x] **Pydantic Canonical Models:** Write the Pydantic classes for `Company` and `License`. Ensure you use `field_validators` to clean data (e.g., stripping white spaces, formatting dates).
* [x] **Great Expectations Setup:** Initialize GE and create 3 basic "Suites" that check for null IDs and valid date ranges on your Silver data.
* [x] **Processing Script:** Write the Python script that reads the CSV, applies the YAML mapping, runs Pydantic validation, and triggers the GE checkpoint.

### Phase 3: Entity Resolution & Vault Prep (Day 2 - AM)

* [ ] **Integrate ER Framework:** Hook up your existing Entity Resolution framework. It should intake the "Silver" data and output a mapping table: `(Source_System, Source_ID) -> Golden_ID`.
* [ ] **The Dynamic Converter:** Build the logic that takes a Pydantic object and:
* Calculates a **Business Key Hash** (for the Hub).
* Calculates a **Hash Diff** (for the Satellite).
* Prepares a dictionary for the Delta Table load.



### Phase 4: The "Gold" Data Vault (Day 2 - PM)

* [ ] **The Business Vault View:** Write a Spark SQL or DuckDB view that joins the Satellites and applies "Survivorship" (e.g., "Use Federal name as the primary name"). **This is your Golden Record.**

### Phase 5: Orchestration & Polishing (Day 3 - AM)

* [ ] **Airflow DAG Construction:** Build the DAG. Ensure you have clear tasks: `ingest` -> `validate` -> `run_er` -> `load_vault` -> `refresh_gold_view`.
* [x] **Audit Logging:** Add print statements or logs in your DAG that show how many records were "Quarantined" vs "Loaded."
* [ ] **README.md (Crucial):**
* Draw the architecture diagram (use Mermaid.js or an image).
* Explain **why** you chose Data Vault 2.0 (Auditability, Traceability).
* Explain the **Dynamic Pydantic Converter** as a scaling framework.





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