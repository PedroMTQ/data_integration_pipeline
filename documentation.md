# Data Integration Pipeline Documentation

## Overview

The Data Integration Pipeline is a Python-based ETL (Extract, Transform, Load) system designed to process, validate, and integrate data from multiple sources. It follows a medallion architecture with bronze (raw), silver (processed), and gold (integrated) layers.

## Architecture

The Data Integration Pipeline follows a medallion architecture with three main layers: bronze (raw data), silver (processed data), and gold (integrated records). The architecture is designed to ensure data quality, traceability, and efficient processing.

### Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│   ┌─────────────┐    ┌─────────────┐    ┌───────────────────────────────────┐  │
│   │             │    │             │    │                                   │  │
│   │  Bronze     │    │  Bronze     │    │  Bronze                         │  │
│   │  (CSV/JSON) │    │  (CSV/JSON) │    │  (CSV/JSON)                     │  │
│   │             │    │             │    │                                   │  │
│   └──────┬──────┘    └──────┬──────┘    └─────────────┬─────────────────┘  │
│          │                 │                         │                   │
│          ▼                 ▼                         ▼                   │
│   ┌─────────────┐    ┌─────────────┐    ┌───────────────────────────────────┐  │
│   │             │    │             │    │                                   │  │
│   │  Silver     │    │  Silver     │    │  Silver                         │  │
│   │  (Delta)    │    │  (Delta)    │    │  (Delta)                        │  │
│   │             │    │             │    │                                   │  │
│   └──────┬──────┘    └──────┬──────┘    └─────────────┬─────────────────┘  │
│          │                 │                         │                   │
│          └─────────┬───────┘                         │                   │
│                    │                                 │                   │
│                    ▼                                 ▼                   │
│            ┌───────────────────────────────────────────────────────┐       │
│            │                                                       │       │
│            │            Entity Resolution (Splink)               │       │
│            │                                                       │       │
│            └─────────────────────┬─────────────────────────────┘       │
│                                  │                                       │
│                                  ▼                                       │
│                    ┌───────────────────────────────────────────┐       │
│                    │                                       │       │
│                    │            Integrated Records            │       │
│                    │                                       │       │
│                    └─────────────────────┬─────────────────┘       │
│                                          │                           │
│                                          ▼                           │
│                            ┌───────────────────────────────────────────┐  │
│                            │                                       │  │
│                            │            Gold Records                │  │
│                            │                                       │  │
│                            └───────────────────────────────────────────┘  │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

### Tools and Technologies

#### 1. Storage Layer

**S3 (MinIO)**
- **Purpose**: Scalable object storage for raw and processed data
- **Usage**: Stores bronze, silver, and gold layer data
- **Why**: Provides durable, scalable storage with versioning capabilities
- **Implementation**: `S3Client` class handles all S3 operations

**Delta Lake**
- **Purpose**: ACID-compliant data lake for versioned data storage
- **Usage**: Stores silver and gold layer data with transaction support
- **Why**: Enables time travel, schema evolution, and efficient updates
- **Implementation**: `DeltaClient` class manages Delta Lake operations

#### 2. Processing Layer

**PyArrow**
- **Purpose**: In-memory data processing and schema management
- **Usage**: Data transformation, schema conversion, and batch processing
- **Why**: High-performance, zero-copy data processing with schema enforcement
- **Implementation**: Used throughout for data manipulation

**DuckDB**
- **Purpose**: Embedded analytical database for efficient querying
- **Usage**: Intermediate data storage and complex joins
- **Why**: Fast, in-process SQL engine with excellent PyArrow integration
- **Implementation**: `DuckdbClient` class for data persistence and querying

**Splink**
- **Purpose**: Probabilistic entity resolution
- **Usage**: Linking records across different data sources
- **Why**: Handles fuzzy matching and deduplication at scale
- **Implementation**: `SplinkClient` class for entity resolution

#### 3. Data Quality

**Great Expectations**
- **Purpose**: Data validation and quality checks
- **Usage**: Auditing silver layer data for quality issues
- **Why**: Provides comprehensive data validation framework
- **Implementation**: `DataAuditor` class for data quality checks

**Pydantic**
- **Purpose**: Data validation and schema enforcement
- **Usage**: Defines data models for each layer
- **Why**: Ensures data consistency and type safety
- **Implementation**: Data models in `data_models/` directory

#### 4. Orchestration

**Custom Python Jobs**
- **Purpose**: Pipeline orchestration
- **Usage**: Defines and executes pipeline jobs
- **Why**: Provides flexibility and control over execution
- **Implementation**: Job classes in `jobs/` directory

### Data Flow

#### 1. Bronze Layer (Raw Data)

**Input**: Raw data files (CSV, JSON) from various sources
**Storage**: S3 bucket in `bronze/` folder
**Processing**:
- Files are uploaded using `UploadBronzeJob`
- No transformation at this stage
- Data is stored as-is for auditability

**Example**:
```
s3://bucket/bronze/business_entity_registry/business_entity_registry.csv
s3://bucket/bronze/licenses_registry/licenses_registry.csv
s3://bucket/bronze/sub_contractors_registry/sub_contractors_registry.csv
```

#### 2. Silver Layer (Processed Data)

**Input**: Bronze layer data
**Storage**: Delta Lake tables in S3 `silver/` folder
**Processing**:
- Data is read from bronze using `S3FileReader`
- Validated against Pydantic models
- Transformed and cleaned
- Written to Delta Lake using `DeltaClient`
- Invalid records are written to error folder

**Example**:
```
s3://bucket/silver/business_entity_registry/business_entity_registry.delta/
s3://bucket/silver/licenses_registry/licenses_registry.delta/
s3://bucket/silver/sub_contractors_registry/sub_contractors_registry.delta/
```

**Key Operations**:
- Schema validation using Pydantic models
- Data cleaning and normalization
- Type conversion and field mapping
- Error handling and logging

#### 3. Entity Resolution

**Input**: Silver layer Delta tables
**Storage**: Intermediate results in DuckDB and S3
**Processing**:
- Data is loaded into DuckDB for efficient processing
- Splink performs probabilistic matching
- Links between entities are identified
- Results are stored in S3 with metadata

**Example**:
```
s3://bucket/entity_resolution/{run_id}/links.parquet
s3://bucket/entity_resolution/{run_id}/metadata.json
s3://bucket/entity_resolution/{run_id}/integrated_records.parquet
```

**Key Operations**:
- Loading data into DuckDB using `DuckdbClient`
- Running Splink entity resolution
- Generating links between related entities
- Creating integrated records from clusters

#### 4. Gold Layer (Integrated Records)

**Input**: Integrated records from entity resolution
**Storage**: Delta Lake tables in S3 `data_mart/` folder
**Processing**:
- Integrated records are processed into final format
- Gold records are created with survivorship rules
- ID bridge tables are created for mapping
- Materialized views are generated for querying

**Example**:
```
s3://bucket/data_mart/gold_business_entity/gold_business_entity.delta/
s3://bucket/data_mart/gold_business_entity/id_bridge.delta/
```

**Key Operations**:
- Creating gold records from integrated data
- Building ID bridge for source-to-gold mapping
- Generating materialized views for efficient querying
- Writing to Delta Lake with partitioning

### Workflow Execution

1. **Data Ingestion**: Raw data is uploaded to bronze layer
2. **Data Processing**: Bronze data is validated and transformed to silver
3. **Data Auditing**: Silver data is audited for quality
4. **Entity Resolution**: Silver data is linked and deduplicated
5. **Record Integration**: Integrated records are created
6. **Gold Creation**: Final gold records are generated

### Data Movement

```
Bronze (S3) → Silver (Delta Lake on S3) → Entity Resolution (DuckDB + S3) → Gold (Delta Lake on S3)
```

Each stage:
- Reads from previous stage
- Processes data with appropriate tools
- Writes to next stage
- Maintains provenance and audit trails

### Key Design Decisions

1. **Medallion Architecture**: Separates concerns and ensures data quality at each stage
2. **Delta Lake**: Provides ACID transactions and time travel capabilities
3. **DuckDB**: Enables efficient in-process SQL operations
4. **Splink**: Handles probabilistic entity resolution at scale
5. **Pydantic**: Ensures data consistency and validation
6. **PyArrow**: Provides high-performance data processing

This architecture provides a robust foundation for data integration with built-in quality checks, efficient processing, and comprehensive audit capabilities.

## Core Components

### 1. Settings and Configuration

**File**: `src/data_integration_pipeline/settings.py`

Central configuration for:
- S3 bucket connections
- Delta Lake table settings
- File paths and folder structures
- Environment variables
- Data processing parameters

### 2. Data Models

**Location**: `src/data_integration_pipeline/core/data_processing/data_models/`

Pydantic models for data validation and schema enforcement:
- `business_entity_registry.py` - Business entity data
- `licenses_registry.py` - License data
- `sub_contractors_registry.py` - Subcontractor data
- `integrated_record.py` - Integrated entity records

### 3. Data Processing

**Location**: `src/data_integration_pipeline/core/data_processing/`

Key modules:
- `model_mapper.py` - Maps file paths to appropriate data models
- `schema_converter.py` - Converts Pydantic models to PyArrow schemas
- `features_extraction/` - Feature extraction utilities
- `mappings.py` - Data mappings (e.g., NAICS codes)

### 4. Entity Resolution

**Location**: `src/data_integration_pipeline/core/entity_resolution/`

- `splink_client.py` - Splink-based entity resolution
- `links_processor.py` - Processes entity links
- `integrated_record.py` - Creates integrated records from linked entities
- `metadata.py` - Tracks entity resolution metadata

### 5. Data Mart

**Location**: `src/data_integration_pipeline/core/data_mart/`

- `gold_records_processor.py` - Processes integrated records into gold layer

### 6. Auditing

**Location**: `src/data_integration_pipeline/core/audits/`

- `data_auditor.py` - Data quality auditing using Great Expectations
- `s3_weighted_data_sampler.py` - Weighted sampling for audit data
- `expectation_data_model.py` - Expectation definitions

### 7. I/O Operations

**Location**: `src/data_integration_pipeline/io/`

- `s3_client.py` - S3 operations
- `delta_client.py` - Delta Lake operations
- `duckdb_client.py` - DuckDB operations
- `file_reader.py` - File reading (local and S3)
- `file_writer.py` - File writing (local and S3)
- `logger.py` - Logging configuration

### 8. Jobs

**Location**: `src/data_integration_pipeline/jobs/`

Pipeline jobs:
- `upload_bronze.py` - Uploads raw data to bronze layer
- `process_bronze_and_load_to_delta_silver.py` - Processes bronze to silver
- `audit_silver.py` - Audits silver layer data
- `run_entity_resolution.py` - Runs entity resolution
- `create_integrated_records.py` - Creates integrated records
- `create_gold_records.py` - Creates gold layer records

## Data Flow

### 1. Bronze Layer

Raw data is uploaded to the bronze layer:
```python
from data_integration_pipeline.jobs.upload_bronze import UploadBronzeJob
job = UploadBronzeJob()
job.run()
```

### 2. Silver Layer

Bronze data is processed and validated:
```python
from data_integration_pipeline.jobs.process_bronze_and_load_to_delta_silver import ProcessBronzetoSilver
job = ProcessBronzetoSilver()
job.run()
```

### 3. Data Auditing

Silver layer data is audited:
```python
from data_integration_pipeline.jobs.audit_silver import AuditSilverDataJob
job = AuditSilverDataJob()
job.run()
```

### 4. Entity Resolution

Entity resolution is performed:
```python
from data_integration_pipeline.jobs.run_entity_resolution import EntityResolutionJob
job = EntityResolutionJob()
job.run()
```

### 5. Integrated Records

Integrated records are created:
```python
from data_integration_pipeline.jobs.create_integrated_records import CreateIntegratedRecords
job = CreateIntegratedRecords()
job.run()
```

### 6. Gold Layer

Gold records are created:
```python
from data_integration_pipeline.jobs.create_gold_records import CreateGoldRecords
job = CreateGoldRecords()
job.run()
```

## Key Features

### Data Validation

- Pydantic models for schema validation
- Custom validators for data cleaning
- Field-level validation rules

### Entity Resolution

- Splink-based probabilistic matching
- Configurable matching rules
- Cluster analysis

### Data Quality

- Great Expectations integration
- Weighted sampling for auditing
- Comprehensive data validation

### Storage

- Delta Lake for versioned data storage
- S3 for scalable storage
- DuckDB for efficient querying

## Configuration

Environment variables can be set in `.env` file:

```
DEBUG=1
S3_ACCESS_KEY=your_access_key
S3_SECRET_ACCESS_KEY=your_secret_key
S3_HOST=localhost
S3_PORT=9000
DATA_BUCKET=your_bucket_name
```

## Running the Pipeline

1. Upload raw data:
```bash
python -m data_integration_pipeline.jobs.upload_bronze
```

2. Process to silver:
```bash
python -m data_integration_pipeline.jobs.process_bronze_and_load_to_delta_silver
```

3. Audit silver data:
```bash
python -m data_integration_pipeline.jobs.audit_silver
```

4. Run entity resolution:
```bash
python -m data_integration_pipeline.jobs.run_entity_resolution
```

5. Create integrated records:
```bash
python -m data_integration_pipeline.jobs.create_integrated_records
```

6. Create gold records:
```bash
python -m data_integration_pipeline.jobs.create_gold_records
```

## Testing

Test data is located in `tests/data/` and includes:
- `business_entity_registry.csv`
- `licenses_registry.csv`
- `sub_contractors_registry.csv`

## Dependencies

Key dependencies:
- `pydantic` - Data validation
- `pyarrow` - Data processing
- `splink` - Entity resolution
- `great_expectations` - Data quality
- `duckdb` - Query engine
- `deltalake` - Delta Lake support
- `boto3` - S3 operations

## Development

The project follows standard Python development practices:
- Type hints throughout
- Pydantic for data validation
- Modular design for maintainability
- Comprehensive logging

## Future Improvements

- Enhanced parallel processing
- More sophisticated entity resolution
- Additional data quality checks
- Better error handling and recovery
- Monitoring and alerting integration
