import os
from datetime import datetime, timezone
from typing import Union, Iterator
from collections.abc import Iterable
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
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
import pyarrow.dataset as ds
import pyarrow.compute as pc


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

    def get_current_version(self, table_name: str) -> int:
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            return dt.version()
        except Exception as e:
            print(f'Error reading changes for {table_name} due to {e}')
            return 0

    def get_data_history(self, table_name: str):
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            table = dt.load_cdf(starting_version=1, ending_version=dt.version()).read_all()
            pt = pl.from_arrow(table)
            print(pt.sort('_commit_version', descending=True))
        except Exception as e:
            print(f'Error reading changes for {table_name} due to {e}')
            raise

    @staticmethod
    def __prepare_data(data: pa.Table, primary_key: Union[str, Iterable], partition_key: str) -> pa.Table:
        if not primary_key:
            raise Exception('Missing primary_key')
        now = datetime.now(timezone.utc)
        df = pl.from_arrow(data)
        if partition_key:
            df = df.with_columns(pl.col(partition_key).fill_null(UNKNOWN_PARTITION_STR))
        if isinstance(primary_key, str):
            exclude = {primary_key, HASH_DIFF_COLUMN, LDTS_COLUMN}
        elif isinstance(primary_key, Iterable):
            exclude = {HASH_DIFF_COLUMN, LDTS_COLUMN} | set(primary_key)
        columns_to_hash = [name for name, dtype in df.schema.items() if name not in exclude and not dtype.is_nested()]

        df = df.with_columns(
            [
                # sets hash diff
                pl.concat_str(pl.col(columns_to_hash)).hash().cast(pl.String).alias(HASH_DIFF_COLUMN),
                # sets load timestamp
                pl.lit(now).alias(LDTS_COLUMN),
            ]
        )
        data = df.to_arrow()
        return data

    def write_overwrite(
        self, s3_path: str, data: pa.Table, primary_key: Union[str, Iterable] = None, partition_key: str = None, add_metadata_columns: bool = True
    ):
        """
        Writes the data to the Delta table using 'overwrite' mode.
        This replaces the entire table content but maintains version history.
        """
        uri = self._get_uri(s3_path)
        if add_metadata_columns:
            data = self.__prepare_data(data=data, primary_key=primary_key, partition_key=partition_key)
        write_deltalake(
            uri,
            data=data,
            mode='overwrite',
            partition_by=[partition_key] if partition_key else None,
            storage_options=self.storage_options,
            # schema_mode="overwrite" allows schema evolution if the integrated record model changes
            schema_mode='overwrite',
        )
        logger.info(f'Overwrote table {s3_path} with {len(data)} integrated records.')

    def write(
        self, s3_path: str, data: pa.Table, primary_key: Union[str, Iterable] = None, partition_key: str = None, add_metadata_columns: bool = True
    ):
        """
        Main entry point. Performs an idempotent upsert using hash-diffing.
        """
        uri = self._get_uri(s3_path)
        if add_metadata_columns:
            data = self.__prepare_data(data=data, primary_key=primary_key, partition_key=partition_key)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            write_deltalake(
                uri,
                data=data,
                partition_by=[partition_key] if partition_key else None,
                storage_options=self.storage_options,
            )
            logger.info(f'Initialized new table {s3_path} with {len(data)} records.')
            return
        dt = DeltaTable(uri, storage_options=self.storage_options)
        mapping = {col: f'source.{col}' for col in data.schema.names if col != primary_key}
        if isinstance(primary_key, str):
            base_predicate = f'target.{primary_key} = source.{primary_key}'
        elif isinstance(primary_key, Iterable):
            base_predicate = []
            for k in primary_key:
                base_predicate.append(f'target.{k} = source.{k}')
            base_predicate = ' AND '.join(base_predicate)

        if partition_key:
            base_predicate += f' AND target.{partition_key} = source.{partition_key}'
        (
            dt.merge(source=data, predicate=base_predicate, source_alias='source', target_alias='target')
            .when_matched_update(
                updates=mapping,
                # This ensures we don't write a new version if the data is identical
                predicate=f'target.{HASH_DIFF_COLUMN} != source.{HASH_DIFF_COLUMN}',
            )
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info(f'Upserted batch into {s3_path}.')

    def read(
        self, table_path: str, columns: list = None, keys: list = None, key_column: str = None, version: Union[int, datetime] = None
    ) -> Iterator[pa.RecordBatch]:
        """
        Direct Arrow streaming from Delta Lake.
        Zero Polars, Zero memory overhead.
        """
        data_model = ModelMapper.get_data_model(table_path)
        partition_key = None
        if data_model:
            partition_key = data_model._partition_key
        uri = self._get_uri(table_path)
        dt = DeltaTable(uri, version=version, storage_options=self.storage_options)
        all_column_names = [f.name for f in dt.schema().fields]
        projection_cols = columns if columns else all_column_names
        projection = {col: ds.field(col) for col in projection_cols}
        if partition_key and partition_key in projection:
            projection[partition_key] = pc.if_else(
                pc.equal(ds.field(partition_key), UNKNOWN_PARTITION_STR), pa.scalar(None, type=pa.string()), ds.field(partition_key)
            )
        filter_expr = None
        if keys and key_column:
            filter_expr = ds.field(key_column).isin(keys)
        return dt.to_pyarrow_dataset().to_batches(columns=projection, filter=filter_expr, batch_size=self.batch_size)

    def rollback(self, table_name: str, version: int = None, timestamp: datetime = None):
        """Restores table to a previous state using Delta Time Travel."""
        if version is None and timestamp is None:
            raise Exception('Missing version and timestamp')
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        if version is not None:
            dt.restore(version)
        elif timestamp is not None:
            dt.restore(timestamp)
        logger.info(f'Rolled back DeltaTable {DELTA_TABLE_URI} to {version or timestamp}')

    def read_table(self, table_name) -> pl.DataFrame:
        # this is generally never used, mostly just for debugging purposes
        return pl.from_arrow(pa.Table.from_batches(self.read(table_name)))


if __name__ == '__main__':
    client = DeltaClient()
    # data = client.read(
    #     table_path="data_mart/gold_business_entity/gold_business_entity.delta",
    # )
    # print(pa.Table.from_batches(data).shape)
    table_path = 'silver/business_entity_registry/records.delta'
    data = client.read(
        table_path=table_path,
    )
    print(client.get_current_version(table_path))
    print(client.get_data_history(table_path))

    # import polars as pl
    # table = pa.Table.from_batches(data)
    # print(table.shape)
    # client.write(table_path, data=table, primary_key='entity_id', partition_key='city')
    # print(client.get_current_version(table_path))
    # print(table.shape)
    # writer = LocalFileWriter(file_path=f"{TEMP}/gold_business_entity/gold_business_entity.parquet")
    # for batch in data:
    #     writer.write_table(batch)
    # print(batch)
    # data_sources = ["business_entity_registry", "licenses_registry", "sub_contractors_registry"]
    # for data_source in data_sources:
    #     data = client.read(
    #         table_path=f"silver/{data_source}/{data_source}.delta",
    #     )
    #     writer = LocalFileWriter(file_path=f"{TEMP}/silver/{data_source}.parquet")
    #     for batch in data:
    #         writer.write_table(batch)
    #     # print(batch)
