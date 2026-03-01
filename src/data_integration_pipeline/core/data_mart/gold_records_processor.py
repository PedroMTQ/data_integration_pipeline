from typing import Iterable
import pyarrow as pa
import duckdb
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    BaseRecordType,
)
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET, DELTA_TABLE_SUFFIX, DATA_MART_DATA_FOLDER
import os
from typing import Type
import polars as pl
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from pathlib import Path

class GoldRecordsProcessor:
    def __init__(self, data_model: Type[BaseRecordType]):
        self.data_model = data_model
        gold_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f'{data_model._data_source}{DELTA_TABLE_SUFFIX}')
        bridge_s3_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, f'id_bridge{DELTA_TABLE_SUFFIX}')
        self.gold_s3_path = gold_s3_path
        self.bridge_s3_path = bridge_s3_path
        self.delta_client = DeltaClient()
        self.db_path = os.path.join(DATA_MART_DATA_FOLDER, data_model._data_source, 'data_mart.duckdb')
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _create_id_bridge(self, gold_data: pa.Table):
        """
        Creates a flat mapping between source IDs and the Integrated ID.
        """
        # 1. Load into Polars for easy list manipulation
        df = pl.from_arrow(gold_data)
        # 2. Extract Anchor IDs
        anchors = df.select([
            pl.col("anchor_entity_id").alias("source_id"),
            pl.col("integrated_entity_id")
        ])
        # 3. Extract and Explode Alternative IDs
        # We assume alt_entity_ids is a list[str] in your Arrow table
        alts = df.select([
            pl.col("alt_entity_ids"),
            pl.col("integrated_entity_id")
        ]).explode("alt_entity_ids").rename({"alt_entity_ids": "source_id"})
        # 4. Combine, drop nulls/duplicates
        bridge_df = pl.concat([anchors, alts]).filter(
            pl.col("source_id").is_not_null()
        ).unique()
        # 5. Write to the bridge S3 path
        self.delta_client.write_overwrite(
            s3_path=self.bridge_s3_path,
            data=bridge_df.to_arrow(),
            primary_key="source_id"
        )
        logger.info(f"ID Bridge synced to {self.bridge_s3_path}")

    @staticmethod
    def create_gold_master(table: pa.Table):
        '''
        creates the gold record from the integrated records. we don't use pydantic here to avoid the row-by-row processing overhead 
        '''
        df = pl.from_arrow(table)
        gold_master = df.select([
            pl.col("anchor_entity").struct.field("entity_id").alias("entity_id"),
            pl.col("anchor_entity").struct.field("global_score").alias("anchor_score"),
            pl.col("anchor_entity").struct.field("vendor_id").cast(pl.String).alias("vendor_id"),
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
        Maps: (entity_id, data_source) -> gold_record_id (Anchor's ID)
        """
        df = pl.from_arrow(data)

        anchors = df.select([
            pl.col("anchor_entity").struct.field("entity_id").alias("entity_id"),
            pl.col("anchor_entity").struct.field("data_source").alias("data_source"),
            pl.col("anchor_entity").struct.field("entity_id").alias("gold_record_id"),
            pl.lit(True).alias("is_anchor")
        ])

        alts = df.select([
            pl.col("alt_entities"),
            pl.col("anchor_entity").struct.field("entity_id").alias("gold_record_id")
        ]).explode("alt_entities").select([
            pl.col("alt_entities").struct.field("entity_id").alias("entity_id"),
            pl.col("alt_entities").struct.field("data_source").alias("data_source"),
            pl.col("gold_record_id"),
            pl.lit(False).alias("is_anchor")
        ])

        bridge = pl.concat([anchors, alts]).filter(
            pl.col("entity_id").is_not_null()
        ).unique(subset=["entity_id", "data_source"])

        bridge = bridge.with_columns([
            pl.col("entity_id").cast(pl.String),
            pl.col("data_source").cast(pl.String),
            pl.col("gold_record_id").cast(pl.String)
        ])

        return bridge.to_arrow()

    def process_data(self, s3_path: pa.Table):
        with S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET, as_table=True) as reader:
            data = pa.concat_tables(reader)
            gold_data = self.create_gold_master(data)
            self.delta_client.write_overwrite(s3_path=self.gold_s3_path, data=gold_data, primary_key=self.data_model._primary_key, partition_key=self.data_model._partition_key)
            bridge_data = self.create_bridge_data(data)
            self.delta_client.write_overwrite(s3_path=self.bridge_s3_path, data=bridge_data, add_metadata_columns=False)
        view_name = self.create_materialzied_view(bridge_table=pa.Table.from_batches(self.delta_client.read(table_path=self.bridge_s3_path)),
                                                  gold_table=pa.Table.from_batches(self.delta_client.read(table_path=self.gold_s3_path)))
        return view_name

    def create_materialzied_view(self, bridge_table: pa.Table, gold_table: pa.Table) -> str:
        with duckdb.connect(self.db_path) as connection:
            connection.register("bridge", bridge_table)
            connection.register("gold", gold_table)
            view_name = f"{self.data_model._data_source}_unified_view"
            unified_view_query = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT
                        -- 1. Explicitly defined Bridge context
                        b.entity_id AS source_entity_id,
                        b.data_source AS original_source,
                        b.is_anchor,
                        b.gold_record_id AS integrated_id,
                        -- 2. Dynamically pull ALL other fields from Gold
                        -- We exclude 'entity_id' because it's already in the bridge as gold_record_id
                        g.* EXCLUDE (entity_id)
                    FROM bridge b
                    JOIN gold g ON b.gold_record_id = g.entity_id;
                    """
            connection.execute(unified_view_query)
        return view_name
