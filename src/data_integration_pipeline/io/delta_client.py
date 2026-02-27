import os
from datetime import datetime, timezone
from typing import Iterable, Union

import polars as pl
import pyarrow as pa
from data_integration_pipeline.io.logger import logger
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from data_integration_pipeline.settings import (
    DELTA_CLIENT_BATCH_SIZE,
    DELTA_TABLE_URI,
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    STORAGE_OPTIONS,
    UNKNOWN_PARTITION_STR,
)


class DeltaClient:
    """
    Client to write pa.Tables into delta parquet tables
    """

    def __init__(
        self,
        base_path: str = DELTA_TABLE_URI,
        storage_options: dict = STORAGE_OPTIONS,
        batch_size: int = DELTA_CLIENT_BATCH_SIZE,
    ):
        """
        Initializes the client.
        base_path: e.g., 's3://my-bucket/stg/' or '/local/path/stg/'
        """
        self.base_path = base_path
        self.storage_options = storage_options or {}
        self.batch_size = batch_size

    def _get_uri(self, table_name: str) -> str:
        return os.path.join(self.base_path, table_name)

    def get_data_history(self, table_name: str):
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            table = dt.load_cdf(starting_version=1, ending_version=dt.version()).read_all()
            pt = pl.from_arrow(table)
            print(pt.sort("_commit_version", descending=True))
        except Exception as e:
            print(f"Error reading changes for {table_name} due to {e}")
            raise e

    @staticmethod
    def __prepare_data(data: pa.Table, upsert_key: str, partition_key: str) -> pa.Table:
        now = datetime.now(timezone.utc)
        df = pl.from_arrow(data)
        if partition_key:
            df = df.with_columns(pl.col(partition_key).fill_null(UNKNOWN_PARTITION_STR))
        exclude = {upsert_key, HASH_DIFF_COLUMN, LDTS_COLUMN}
        columns_to_hash = [c for c in df.columns if c not in exclude]
        df = df.with_columns(
            [
                # sets hash diff
                pl.concat_str(pl.col(columns_to_hash)).hash().cast(pl.Utf8).alias(HASH_DIFF_COLUMN),
                # sets load timestamp
                pl.lit(now).alias(LDTS_COLUMN),
            ]
        )
        data = df.to_arrow()
        return data

    def write(self, s3_path: str, data: pa.Table, upsert_key: str, partition_key: str = None):
        """
        Main entry point. Performs an idempotent upsert using hash-diffing.
        """
        uri = self._get_uri(s3_path)
        data = self.__prepare_data(data=data, upsert_key=upsert_key, partition_key=partition_key)
        # 2. Handle Initial Table Creation
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            write_deltalake(
                uri,
                data=data,
                partition_by=[partition_key] if partition_key else None,
                storage_options=self.storage_options,
            )
            logger.info(f"Initialized new table {s3_path} with {len(data)} records.")
            return
        dt = DeltaTable(uri, storage_options=self.storage_options)
        mapping = {col: f"source.{col}" for col in data.schema.names if col != upsert_key}
        base_predicate = f"target.{upsert_key} = source.{upsert_key}"
        if partition_key:
            base_predicate += f" AND target.{partition_key} = source.{partition_key}"
        (
            dt.merge(source=data, predicate=base_predicate, source_alias="source", target_alias="target")
            .when_matched_update(
                updates=mapping,
                # This ensures we don't write a new version if the data is identical
                predicate=f"target.{HASH_DIFF_COLUMN} != source.{HASH_DIFF_COLUMN}",
            )
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info(f"Upserted batch into {s3_path}.")

    def read(self, table_name: str, columns: list = None, keys: list = None, key_column: str = None, version: Union[int, datetime] = None) -> Iterable[pa.Table]:
        """
        Unified read method using Polars.
        Handles fragmentation by re-batching and lookups via predicate pushdown.
        """
        if key_column or keys:
            if not (key_column and keys):
                raise Exception(f'When filtering data, you need to provide the keys and key_column') 
        uri = self._get_uri(table_name)
        lf = pl.scan_delta(uri, storage_options=self.storage_options, version=version)
        if columns:
            lf = lf.select(columns)
        if keys:
            lf = lf.filter(pl.col(key_column).is_in(keys))
        df = lf.collect()
        for sub_df in df.iter_slices(n_rows=self.batch_size):
            yield sub_df.to_arrow()

    def rollback(self, table_name: str, version: int = None, timestamp: datetime = None):
        """Restores table to a previous state using Delta Time Travel."""
        if version is None and timestamp is None:
            raise Exception("Missing version and timestamp")
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        if version is not None:
            dt.restore(version)
        elif timestamp is not None:
            dt.restore(timestamp)
        logger.info(f"Rolled back DeltaTable {DELTA_TABLE_URI} to {version or timestamp}")


if __name__ == "__main__":
    client = DeltaClient()
    data = client.read(
        table_name="silver/business_entity_registry/business_entity_registry.delta",
    )
    for i in data:
        print(i.shape)
