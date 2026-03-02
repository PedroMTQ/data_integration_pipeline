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
    COMPOSITE_KEY_SEP,
    S3_SECRET_ACCESS_KEY,
    COMPOSITE_ID_STR
)
import os
from typing import Type
import polars as pl
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.io.file_writer import S3FileWriter
from pathlib import Path


class DuplicatesProcessor:
    def __init__(self, data_model: Type[BaseRecordType]):
        self.data_model = data_model
        self.primary_key = data_model._primary_key
        self.delta_client = DeltaClient()


    @staticmethod
    def create_deduplicated_data(data: pa.Table) -> pa.Table:
        """
        Creates a flat cross-walk table.
        Maps: (entity_id, data_source) -> anchor_entity_id (Anchor's ID)
        """
        df = pl.from_arrow(data)
        deduplicated_data = (
            df.select([
               pl.col("anchor_entity").struct.field("entity_id").alias("anchor_entity_id"),
               pl.col("anchor_entity").struct.field("data_source").alias("anchor_data_source"),
               pl.all()
            ])
            .sort(
                by=["is_active", "global_score"], 
                descending=[True, True]
            )
            .unique(subset=["anchor_entity_id", "anchor_data_source"], keep="first")
        )
        return deduplicated_data.to_arrow()

    def load_data(self, s3_path: str):
        count_deduplicated_data = 0
        count_total = 0
        reader = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET, as_table=True)
        with reader as stream_in:
            for i, batch in enumerate(reader):
                deduplicated_data = self.create_deduplicated_data(batch)
                count_total += len(batch)
                count_deduplicated_data += len(deduplicated_data)
                yield deduplicated_data
        logger.info(f"Consumed {count_total} total records and {count_deduplicated_data} deduplicated records")


    def process_data(self, input_path: str, output_path: str):
        count_deduplicated_data = 0
        count_total = 0
        data_streamer = self.load_data(input_path)
        writer = S3FileWriter(s3_path=output_path, bucket_name=DATA_BUCKET)
        with writer as stream_out:
            stream_out.write_table(data_streamer)
