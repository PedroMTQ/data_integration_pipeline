import os
from datetime import datetime, timezone
from typing import Iterable

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from data_integration_pipeline.io.logger import logger
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from data_integration_pipeline.settings import (
    DELTA_CLIENT_BATCH_SIZE,
    DELTA_TABLE_URI,
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    PRIMARY_KEY_COLUMN,
    STORAGE_OPTIONS,
)


class DeltaVaultClient:
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

    def write(self, table_name: str, data: pa.Table):
        """Main entry point that decides the write strategy."""
        uri = self._get_uri(table_name)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            write_deltalake(uri, data=data, storage_options=self.storage_options)
            logger.info(f"Added {len(data)} new records to {table_name}")
            return
        if HASH_DIFF_COLUMN in data.schema.names:
            logger.debug(f"Writing data to satellite {table_name}")
            self._write_to_satellite(table_name=table_name, data=data)
        else:
            logger.debug(f"Writing data to Hub/Link {table_name}")
            self._write_to_hub_or_link(table_name=table_name, data=data)

    def _write_to_hub_or_link(self, table_name: str, data: pa.Table):
        """Strategy for Hubs/Links: Write only if Hash Key is new."""
        uri = self._get_uri(table_name)
        # 1. Convert incoming to Polars
        df_incoming = pl.from_arrow(data)
        # 2. Get existing keys (Lazy Scan with Predicate Pushdown)
        # We only read the 'hash_key' column for keys present in our current batch
        incoming_keys = df_incoming.get_column(PRIMARY_KEY_COLUMN).unique()
        existing_keys = (
            pl.scan_delta(uri, storage_options=self.storage_options)
            .filter(pl.col(PRIMARY_KEY_COLUMN).is_in(incoming_keys))
            .select(PRIMARY_KEY_COLUMN)
            .collect()
        )
        # 3. The "Anti-Join": Keep only records where hash_key is NOT in existing_keys
        new_records = df_incoming.join(existing_keys, on=PRIMARY_KEY_COLUMN, how="anti")
        # 4. Append
        if len(new_records):
            write_deltalake(
                uri,
                data=new_records.to_arrow(),
                mode="append",
                storage_options=self.storage_options,
                schema_mode="merge",
            )
            logger.info(f"Added {len(new_records)} new keys to {table_name}")
        else:
            logger.debug(f"No new keys to add for {table_name}")

    def _write_to_satellite(self, table_name: str, data: pa.Table):
        """Strategy for Satellites: Write only if hash_diff has changed."""
        uri = self._get_uri(table_name)
        # 1. Convert incoming Arrow to Polars (Zero-copy)
        df_incoming = pl.from_arrow(data)
        # 2. Get the latest 'state' from Delta
        # We scan (lazy) and filter for only the keys in our batch to be efficient
        incoming_keys = df_incoming.get_column(PRIMARY_KEY_COLUMN).unique()
        # 1. Turn the in-memory DataFrame into a "LazyFrame" so Polars can optimize the query
        to_append = (
            df_incoming.lazy()
            # 2. Left join the incoming data against the existing history in Delta Lake
            .join(
                # --- SUBQUERY: Get the latest state of each record in the Delta Table ---
                pl.scan_delta(uri, storage_options=self.storage_options)
                # Performance: Only read Parquet files containing keys in our current batch
                .filter(pl.col(PRIMARY_KEY_COLUMN).is_in(incoming_keys))
                # Projection: Only pull the columns needed to detect a change
                .select([PRIMARY_KEY_COLUMN, HASH_DIFF_COLUMN, LDTS_COLUMN])
                # Sort by time so the 'head(1)' below captures the most recent version
                .sort(LDTS_COLUMN, descending=True)
                # Group by the ID and grab the top row (the current 'active' state)
                .group_by(PRIMARY_KEY_COLUMN)
                .head(1),
                # --- JOIN LOGIC ---
                on=PRIMARY_KEY_COLUMN,
                how="left",  # Keep all incoming records
                suffix="_existing",  # Label existing columns to avoid naming collisions
            )
            # 3. Filter: Only keep rows that represent a REAL change
            .filter(
                # CASE A: The Hash Diff is different (The data changed)
                (pl.col(HASH_DIFF_COLUMN) != pl.col(f"{HASH_DIFF_COLUMN}_existing"))
                |
                # CASE B: The existing diff is null (This is a brand new record)
                (pl.col(f"{HASH_DIFF_COLUMN}_existing").is_null())
            )
            # 4. Standardize the Satellite metadata for the new version
            .with_columns(
                [
                    # Stamp the exact time this change was detected and loaded
                    pl.lit(datetime.now(timezone.utc)).alias(LDTS_COLUMN)
                ]
            )
            # 5. Clean up: Remove the temporary '_existing' columns used for comparison
            .select(df_incoming.columns)
            # 6. Execution: Run the plan and return the final data for the Delta append
            .collect()
        )
        # 5. Write changes back to Delta
        if to_append.height > 0:
            write_deltalake(
                uri,
                data=to_append.to_arrow(),
                mode="append",
                storage_options=self.storage_options,
                schema_mode="merge",
            )
            logger.info(f"Satellite Update: {to_append.height} records added to {table_name}.")
        else:
            logger.debug(f"No changes detected for {table_name}. Skipping write.")

    def read(self, table_name: str, columns: list = None, version: int = None, timestamp: datetime = None) -> Iterable[pa.Table]:
        """
        Reads a Delta table directly into a PyArrow Table.
        """
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        if version is not None:
            dt.load_as_version(version)
        elif timestamp is not None:
            dt.load_as_version(timestamp)
        dataset = dt.to_pyarrow_dataset()
        scanner = dataset.scanner(columns=columns, batch_size=self.batch_size)
        for batch in scanner.to_batches():
            yield batch

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

    def get_by_hk(self, table_name: str, hk_value: str):
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        dataset = dt.to_pyarrow_dataset()
        row = dataset.to_table(filter=(ds.field(PRIMARY_KEY_COLUMN) == hk_value)).to_pylist()
        if row:
            return row[0]

    def create_scd_type2_view(uri: str, storage_options: dict, business_key: str):
        """
        Creates a historical timeline view of a Satellite table.
        """
        return (
            # 1. Scan the Delta Table lazily
            pl.scan_deltalake(uri, storage_options=storage_options)
            # 2. Sort by business key and load time to ensure chronological order
            .sort([business_key, "load_datetime"])
            # 3. Add SCD Type 2 Window Functions
            .with_columns(
                [
                    # The start of the record's life is its load time
                    pl.col("load_datetime").alias("valid_from"),
                    # The 'valid_to' is the 'load_datetime' of the NEXT record for this key
                    pl.col("load_datetime")
                    .shift(-1)  # Peek at the next row
                    .over(business_key)  # Only look at the same company
                    .alias("valid_to"),
                ]
            )
            # 4. Add an 'is_current' flag for convenience
            .with_columns([pl.col("valid_to").is_null().alias("is_current")])
            # 5. Fill the final 'valid_to' with a far-future date (optional standard)
            .with_columns([pl.col("valid_to").fill_null(pl.datetime(9999, 12, 31))])
        )


if __name__ == "__main__":
    client = DeltaVaultClient()
    row_dict = client.get_by_hk(
        table_name="companies",
        hk_value="724e176c6b0405f059eb328c95c3b800b7040326ea4e4dec96dab3317679cb22",
    )
    print(row_dict)
