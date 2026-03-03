import pyarrow as pa
import duckdb
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    BaseRecordType,
)
from typing import Literal
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
    def __init__(self, partition_by_keys: list[str]):
        run_id = str(uuid6.uuid7())
        if not partition_by_keys:
            raise Exception(f'We need primary keys for deduplication!')
        self.partition_by_str = ','.join(partition_by_keys)
        self.db_path = os.path.join(TEMP, ENTITY_RESOLUTION_DATA_FOLDER, run_id, "deduped.duckdb")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)


    def _deduplicate_silver(self, input_path: str, output_path: str, connection: duckdb.DuckDBPyConnection):
        connection.execute("INSTALL delta; LOAD delta;")
        schema_sample = connection.execute(f"SELECT * FROM delta_scan('{input_path}') LIMIT 0").arrow()
        columns = schema_sample.schema.names
        order_by_list = []
        if 'is_active' in columns:
            order_by_list.append("is_active DESC")
        order_by_list.append("fill_count DESC")
        order_by_str = ", ".join(order_by_list)
        # this only checks for the null count of the root dict values. it doesn't check for nulls in nested values, which for the silver records is not a problem
        fill_count_expr = " + ".join([f'("{c}" IS NOT NULL)::INT' for c in columns])
        connection.execute(f"""
                COPY (
                    WITH raw_data AS (
                        SELECT * FROM delta_scan('{input_path}')
                    ),
                    scored_data AS (
                        SELECT *,
                        {fill_count_expr} AS fill_count
                        from raw_data

                    )
                    SELECT * EXCLUDE (fill_count) FROM scored_data
                    QUALIFY row_number() OVER (
                        PARTITION BY {self.partition_by_str}
                        ORDER BY {order_by_str}
                    ) = 1
                ) TO '{output_path}' (FORMAT PARQUET);
            """)  
        initial_count = connection.execute(f"SELECT COUNT(*) FROM delta_scan('{input_path}')").fetchone()[0]
        deduplicated_count = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        return {'initial_count': initial_count, 'deduplicated_count': deduplicated_count}

    def _deduplicate_integrated(self, input_path: str, output_path: str, connection: duckdb.DuckDBPyConnection) -> dict:
        schema_sample = connection.execute(f"SELECT * FROM read_parquet('{input_path}') LIMIT 0").arrow()
        columns = schema_sample.schema.names
        order_by_list = []
        if 'is_active' in columns:
            order_by_list.append("is_active DESC")
        if 'global_score' in columns:
            order_by_list.append("global_score DESC")
        if not order_by_list:
            raise Exception('Missing ordering columns for deduplication')
        order_by_str = ", ".join(order_by_list)
        connection.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{input_path}')
                    QUALIFY row_number() OVER (
                        PARTITION BY {self.partition_by_str}
                        ORDER BY {order_by_str}
                    ) = 1
                ) TO '{output_path}' (FORMAT PARQUET);
            """)
        # TODO: we should avoid reading from s3 twice, we could instead just load the data to a temp db and the get the count from disk. Same of the other method
        initial_count = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{input_path}')").fetchone()[0]
        deduplicated_count = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        return {'initial_count': initial_count, 'deduplicated_count': deduplicated_count}


    def run(self, input_path: str, output_path: str, data_type: Literal['silver', 'integrated']):
        endpoint = S3_ENDPOINT_URL.replace("http://", "").replace("https://", "")
        full_input_path = f"s3://{DATA_BUCKET}/{input_path}"
        full_output_path = f"s3://{DATA_BUCKET}/{output_path}"
        with duckdb.connect(self.db_path) as connection:
            connection.execute("INSTALL httpfs; LOAD httpfs;")
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
            if  data_type == 'silver':
                initial_count, deduplicated_count = self._deduplicate_silver(input_path=full_input_path, output_path=full_output_path, connection=connection)
            elif data_type == 'integrated':
                initial_count, deduplicated_count = self._deduplicate_integrated(input_path=full_input_path, output_path=full_output_path, connection=connection)

        logger.info(f"Consumed {initial_count} records from {input_path} and {deduplicated_count} deduplicated records into {output_path}")


