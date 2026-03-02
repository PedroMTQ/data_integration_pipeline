import pyarrow as pa
import duckdb
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    BaseRecordType,
)
from data_integration_pipeline.settings import (
    TEMP,
    DATA_BUCKET,
    DELTA_TABLE_SUFFIX,
    ENTITY_RESOLUTION_DATA_FOLDER,
    S3_ENDPOINT_URL,
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    S3_ACCESS_KEY,
    COMPOSITE_KEY_SEP,
    S3_SECRET_ACCESS_KEY,
    COMPOSITE_ID_STR,
    PARQUET_TABLE_SUFFIX, DELTA_TABLE_SUFFIX
)
import os
from typing import Type
import polars as pl
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.io.file_writer import S3FileWriter
from pathlib import Path
import uuid6


class DuplicatesProcessor:
    '''
    deduplicates silver or integrated records data
    '''
    def __init__(self):
        run_id = str(uuid6.uuid7())
        self.db_path = os.path.join(TEMP, ENTITY_RESOLUTION_DATA_FOLDER, run_id, "deduped.duckdb")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def run(self, input_path: str, output_path: str, partition_by: str):
        endpoint = S3_ENDPOINT_URL.replace("http://", "").replace("https://", "")
        full_input_path = f"s3://{DATA_BUCKET}/{input_path}"
        full_output_path = f"s3://{DATA_BUCKET}/{output_path}"
        extension = Path(input_path).suffix.lower()
        with duckdb.connect(self.db_path) as connection:
            connection.execute("INSTALL httpfs; LOAD httpfs;")

            if  extension== PARQUET_TABLE_SUFFIX:
                read_function = 'read_parquet'
            elif extension == DELTA_TABLE_SUFFIX:
                read_function = 'delta_scan'
                connection.execute("INSTALL delta; LOAD delta;")
            schema_sample = connection.execute(f"SELECT * FROM {read_function}('{full_input_path}') LIMIT 0").arrow()
            cols = schema_sample.schema.names
            print(cols)
            connection.execute(f"""
                CREATE OR REPLACE SECRET minio_secret (
                    TYPE S3,
                    PROVIDER config,
                    KEY_ID '{S3_ACCESS_KEY}',
                    SECRET '{S3_SECRET_ACCESS_KEY}',
                    REGION 'us-east-1',
                    ENDPOINT '{endpoint}',
                    URL_STYLE 'path',
                    USE_SSL 'false'
                );
            """)

            logger.info(f"Starting global deduplication from {full_input_path}")
            connection.execute(f"""
                    COPY (
                        WITH raw_data AS (
                            SELECT row, * FROM {read_function}('{full_input_path}') as row
                        ),
                        scored_data AS (
                            SELECT *,
                            (
                                SELECT count(*) 
                                FROM (SELECT unnest(row)) AS t(val)
                                WHERE t.val IS NOT NULL
                                ) AS fill_count
                            from raw_data

                        )
                        SELECT * EXCLUDE (row) FROM scored_data
                        QUALIFY row_number() OVER (
                            PARTITION BY {partition_by}
                            ORDER BY 
                                -- 1. Active Status
                                COALESCE(is_active, False) DESC,
                                -- 2. Splink's Global Score (if it exists)
                                COALESCE(global_score, 0) DESC,
                                -- 3. Total Non-Null Columns
                                fill_count DESC
                        ) = 1
                    ) TO '{full_output_path}' (FORMAT PARQUET);
                """)
            initial_count = connection.execute(f"SELECT COUNT(*) FROM {read_function}('{full_input_path}')").fetchone()[0]
            deduplicated_count = connection.execute(f"SELECT COUNT(*) FROM {read_function}('{full_output_path}')").fetchone()[0]
        logger.info(f"Consumed {initial_count} records from {input_path} and {deduplicated_count} deduplicated records into {output_path}")
        # os.remove(self.db_path)


