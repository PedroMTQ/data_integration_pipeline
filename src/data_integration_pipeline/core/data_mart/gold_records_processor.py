from typing import Iterable
import pyarrow as pa
import duckdb
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    BaseRecordType,
)
from data_integration_pipeline.settings import TEMP, DATA_BUCKET, DELTA_TABLE_SUFFIX, DATA_MART_DATA_FOLDER, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY
import os
from typing import Type
import polars as pl
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from pathlib import Path

# TODO this is not dynamic at all, so we could have better flexibility with other gold records data models.
class GoldRecordsProcessor:
    def __init__(self, data_model: Type[BaseRecordType]):
        self.data_model = data_model
        self.primary_key = data_model._primary_key
        gold_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f'{data_model._data_source}{DELTA_TABLE_SUFFIX}')
        bridge_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f'id_bridge{DELTA_TABLE_SUFFIX}')
        self.gold_s3_path = gold_s3_path
        self.bridge_s3_path = bridge_s3_path
        self.delta_client = DeltaClient()
        self.db_path = os.path.join(TEMP, DATA_MART_DATA_FOLDER, data_model._data_source, 'data_mart.duckdb')
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def create_gold_master(table: pa.Table):
        '''
        creates the gold record from the integrated records. we don't use pydantic here to avoid the row-by-row processing overhead 
        '''
        df = pl.from_arrow(table)
        gold_master = df.select([
            pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
            pl.col("anchor_entity").struct.field("data_source").alias("anchor_data_source"),
            pl.col("anchor_entity").struct.field("global_score").alias("anchor_score"),
            pl.col("anchor_entity").struct.field("vendor_id").cast(pl.String).alias("anchor_vendor_id"),
            # Core Survived Data
            "company_name",
            "address_1",
            "city",
            "postal_code",
            "is_active",
            # Industry Info
            "naics_code",
            "naics_code_label",
            "certification_type",
            "trade_specialty",
            pl.col("licenses"),
        ])
        return gold_master

    @staticmethod
    def create_bridge_data(data: pa.Table) -> pa.Table:
        """
        Creates a flat cross-walk table.
        Maps: (entity_id, data_source) -> anchor_entity_id (Anchor's ID)
        """
        df = pl.from_arrow(data)

        anchors = df.select([
            pl.col("anchor_entity").struct.field("entity_id").alias("entity_id"),
            pl.col("anchor_entity").struct.field("data_source").alias("data_source"),
            pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
            pl.lit(True).alias("is_anchor")
        ])
        alt_struct_type = pl.Struct([
                pl.Field("entity_id", pl.String),
                pl.Field("data_source", pl.String)
            ])
        alts = (
                df.select(
                    pl.col("alt_entities").cast(pl.List(alt_struct_type)),
                    pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id")
                          )
                .filter(
                    pl.col("alt_entities").is_not_null() &
                    (pl.col("alt_entities").list.len() > 0)
                )
                .explode("alt_entities")
                .select([
                    pl.col("alt_entities").struct.field("entity_id").alias("entity_id"),
                    pl.col("alt_entities").struct.field("data_source").alias("data_source"),
                    pl.col("anchor_entity_id"),
                    pl.lit(False).alias("is_anchor")
                ])
            )
        print('alts', alts)

        bridge = (
            pl.concat([anchors, alts])
            .filter(pl.col("entity_id").is_not_null())
            .sort("is_anchor", descending=True) 
            .unique(subset=["entity_id", "data_source"], keep="first")
        )
        print('bridge', bridge)
        bridge = bridge.with_columns([
            pl.col("entity_id").cast(pl.String),
            pl.col("data_source").cast(pl.String),
            pl.col("anchor_entity_id").cast(pl.String)
        ])

        return bridge.to_arrow()

    def process_data(self, s3_path: str):
        count_gold = 0
        count_bridge = 0
        with S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET, as_table=True) as reader:
            for i, batch in enumerate(reader):
                gold_chunk = self.create_gold_master(batch)
                bridge_chunk = self.create_bridge_data(batch)
                count_gold += len(gold_chunk)
                count_bridge += len(bridge_chunk)
                if i == 0:
                    self.delta_client.write_overwrite(s3_path=self.gold_s3_path, data=gold_chunk, primary_key=self.data_model._primary_key, partition_key=self.data_model._partition_key)
                    self.delta_client.write_overwrite(s3_path=self.bridge_s3_path, data=bridge_chunk, add_metadata_columns=False)
                else:
                    self.delta_client.write(s3_path=self.gold_s3_path, data=gold_chunk, primary_key=self.data_model._primary_key, partition_key=self.data_model._partition_key)
                    self.delta_client.write(s3_path=self.bridge_s3_path, data=bridge_chunk, add_metadata_columns=False)
        logger.info(f'Consumed {count_gold} gold records and {count_bridge} bridge records')
        return self.create_gold_table()

    def create_gold_table(self) -> str:
        """
        Physically joins the S3 data and saves it INTO the local DuckDB file.
        This makes the .duckdb file 100% portable and fast.
        """
        table = f"{self.data_model._data_source}_unified_table"
        bridge_path = f"s3://{DATA_BUCKET}/{self.bridge_s3_path}"
        gold_path = f"s3://{DATA_BUCKET}/{self.gold_s3_path}"
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
                    bridge.entity_id AS entity_id,
                    bridge.anchor_entity_id AS anchor_entity_id,
                    gold.* EXCLUDE (anchor_entity_id)
                FROM delta_scan('{bridge_path}') bridge
                JOIN delta_scan('{gold_path}') gold
                ON bridge.anchor_entity_id = gold.anchor_entity_id;
            """)
            # 3. Add an index to make it lightning fast for the analyst
            connection.execute(f"CREATE INDEX idx_entity_id ON {table} (entity_id);")
            records_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"Created table '{table}' ({records_count} records) stored in {self.db_path}")
            print(connection.execute(f'select * from {table}').pl())
        return table

