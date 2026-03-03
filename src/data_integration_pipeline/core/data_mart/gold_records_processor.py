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
    DATA_MART_DATA_FOLDER,
    S3_ENDPOINT_URL,
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    DELTA_CLIENT_BATCH_SIZE,
)
import os
from typing import Type
import polars as pl
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from pathlib import Path
from data_integration_pipeline.core.data_mart.gold_record import Record

from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper

class GoldRecordsProcessor:
    def __init__(self, data_model: Type[BaseRecordType]):
        self.data_model = data_model
        anchor_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f"anchors{DELTA_TABLE_SUFFIX}")
        bridge_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f"id_bridge{DELTA_TABLE_SUFFIX}")
        gold_records_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f"gold_records{DELTA_TABLE_SUFFIX}")
        self.anchor_s3_path = anchor_s3_path
        self.bridge_s3_path = bridge_s3_path
        self.gold_s3_path = gold_records_s3_path
        self.delta_client = DeltaClient()
        self.db_path = os.path.join(TEMP, DATA_MART_DATA_FOLDER, data_model._data_source, "data_mart.duckdb")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def create_anchor_data(table: pa.Table):
        """
        creates the anchor records from the integrated records. we don't use pydantic here to avoid the row-by-row processing overhead
        """
        df = pl.from_arrow(table)
        anchor_data = df.select(
            [
                pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
                pl.col("anchor_entity").struct.field("data_source").alias("anchor_data_source"),
                pl.col("anchor_entity").struct.field("global_score").alias("anchor_score"),
                pl.col("anchor_entity").struct.field("vendor_id").cast(pl.String).alias("vendor_id"),
                pl.all().exclude(["anchor_entity", "alt_entities"]),
            ]
        )

        return anchor_data

    @staticmethod
    def create_bridge_data(data: pa.Table) -> pa.Table:
        """
        Creates a flat cross-walk table.
        Maps: (entity_id, data_source) -> anchor_entity_id (Anchor's ID)
        """
        df = pl.from_arrow(data)
        anchors = df.select(
            pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
            pl.col("anchor_entity").struct.field("data_source").alias("anchor_data_source"),
            pl.lit('').alias("alt_entity_id"),
            pl.lit('').alias("alt_data_source"),
            pl.lit(True).alias("is_anchor"),
        )
        logger.debug(f"{'-' * 20}{'Anchor entities'}{'-' * 20}\n{anchors}")


        alt_struct_type = pl.Struct([pl.Field("entity_id", pl.String),
                                     pl.Field("data_source", pl.String)])
        alts = (
            df.select(
                pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
                pl.col("anchor_entity").struct.field("data_source").alias("anchor_data_source"),
                pl.col("alt_entities").cast(pl.List(alt_struct_type)),
            )
            .filter(pl.col("alt_entities").is_not_null() & (pl.col("alt_entities").list.len() > 0))
            .explode("alt_entities")
            .select(
                [
                    pl.col('anchor_entity_id'),
                    pl.col('anchor_data_source'),
                    pl.col("alt_entities").struct.field("entity_id").alias("alt_entity_id"),
                    pl.col("alt_entities").struct.field("data_source").alias("alt_data_source"),
                    pl.lit(False).alias("is_anchor"),
                ]
            )
        )
        logger.debug(rf"{'-' * 20}{'Non-anchor entities and their respective anchors'}{'-' * 20}\{alts}")
    
        bridge = (
            pl.concat([anchors, alts])
            # these are not really necessary
            # .sort("is_anchor", descending=True)
            # .unique(subset=["anchor_entity_id", "anchor_data_source", 'alt_entity_id', 'alt_data_source'], keep="first")
        )
        # bridge = bridge.with_columns(
        #     [pl.col("entity_id").cast(pl.String), pl.col("data_source").cast(pl.String), pl.col("anchor_entity_id").cast(pl.String)]
        # )
        logger.debug(f"{'-' * 20}{'Bridge table'}{'-' * 20}\n{bridge}")

        return bridge.to_arrow()


    def create_gold_table(self) -> str:
        """
        Physically joins the S3 data and saves it INTO the local DuckDB file.
        This makes the .duckdb file 100% portable and fast.
        """
        table = f"{self.data_model._data_source}_unified_table"
        bridge_path = f"s3://{DATA_BUCKET}/{self.bridge_s3_path}"
        anchor_path = f"s3://{DATA_BUCKET}/{self.anchor_s3_path}"
        endpoint = S3_ENDPOINT_URL.replace("http://", "").replace("https://", "")
        with duckdb.connect(self.db_path) as connection:
            connection.execute("INSTALL httpfs; LOAD httpfs;")
            connection.execute("INSTALL delta; LOAD delta;")
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

            connection.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT
                    bridge.anchor_entity_id AS anchor_entity_id,
                    bridge.anchor_data_source AS anchor_data_source,
                    bridge.alt_entity_id AS alt_entity_id,
                    bridge.alt_data_source AS alt_data_source,
                    bridge.is_anchor AS is_anchor,
                    anchor.* EXCLUDE (anchor_entity_id, anchor_data_source, anchor_score, {HASH_DIFF_COLUMN}, {LDTS_COLUMN})
                FROM delta_scan('{bridge_path}') bridge
                JOIN delta_scan('{anchor_path}') anchor
                ON bridge.anchor_entity_id = anchor.anchor_entity_id;
            """)
            # 3. Add an index to make it lightning fast for the analyst
            connection.execute(f"CREATE INDEX idx_anchor_entity_id ON {table} (anchor_entity_id);")
            connection.execute(f"CREATE INDEX idx_alt_entity_id ON {table} (alt_entity_id);")
            records_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"Created table '{table}' ({records_count} records) stored in {self.db_path}")
        return table

    def write_gold_delta(self, table_name):
        with duckdb.connect(self.db_path) as connection:
            for i, batch in enumerate(connection.execute(f"SELECT * FROM {table_name}").fetch_record_batch(DELTA_CLIENT_BATCH_SIZE)):
                gold_records = []
                for record in batch.to_pylist():
                    record_instance = self.data_model(**record)
                    gold_records.append(record_instance.model_dump())
                table = pa.Table.from_pylist(gold_records, schema=self.data_model._pa_schema)
                if i == 0:
                    self.delta_client.write_overwrite(
                        s3_path=self.gold_s3_path,
                        data=table,
                        primary_key=self.data_model._primary_key,
                        partition_key=self.data_model._partition_key,
                    )
                else:
                    self.delta_client.write(
                        s3_path=self.gold_s3_path,
                        data=table,
                        primary_key=self.data_model._primary_key,
                        partition_key=self.data_model._partition_key,
                    )




    def process_data(self, s3_path: str):
        count_anchor = 0
        count_bridge = 0
        with S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET, as_table=True) as reader:
            for i, batch in enumerate(reader):
                anchor_chunk = self.create_anchor_data(batch)
                bridge_chunk = self.create_bridge_data(batch)
                count_anchor += len(anchor_chunk)
                count_bridge += len(bridge_chunk)
                if i == 0:
                    self.delta_client.write_overwrite(
                        s3_path=self.anchor_s3_path,
                        data=anchor_chunk,
                        primary_key=self.data_model._primary_key,
                        partition_key=self.data_model._partition_key,
                    )
                    self.delta_client.write_overwrite(s3_path=self.bridge_s3_path, data=bridge_chunk, add_metadata_columns=False)
                else:
                    self.delta_client.write(
                        s3_path=self.anchor_s3_path,
                        data=anchor_chunk,
                        primary_key=self.data_model._primary_key,
                        partition_key=self.data_model._partition_key,
                    )
                    self.delta_client.write(s3_path=self.bridge_s3_path, data=bridge_chunk, add_metadata_columns=False)
        logger.info(f"Consumed {count_anchor} anchor and {count_bridge} bridge records")
        gold_data_table = self.create_gold_table()
        self.write_gold_delta(gold_data_table)
        # os.remove(self.db_path)
        
