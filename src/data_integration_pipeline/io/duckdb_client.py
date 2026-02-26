import os
from pathlib import Path
from typing import Iterable, Optional

import duckdb
import pyarrow as pa
from data_integration_pipeline.io.logger import logger


class DuckdbClient:
    def __init__(
        self,
        db_path: str,
        table_name: str,
        batch_size: int = 10_000,
    ):
        self.db_path = db_path
        self.table_name = table_name
        self.batch_size = batch_size
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def __str__(self):
        return f"DuckDB client table:{self.table_name} stored at:{self.db_path}"

    def get_count(self) -> int:
        with duckdb.connect(self.db_path) as conn:
            return conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]

    def exists(self):
        if not os.path.exists(self.db_path):
            return False
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute(f"DESCRIBE {self.table_name}")
                if self.get_count():
                    return True
                else:
                    return False
        except Exception as _:
            return False

    def drop_table(self):
        with duckdb.connect(self.db_path) as connection:
            connection.execute(f"DROP TABLE IF EXISTS {self.table_name}")


    def get_schema(self) -> pa.Schema | None:
        try:
            with duckdb.connect(self.db_path) as connection:
                return connection.execute(f"SELECT * FROM {self.table_name} LIMIT 0").fetch_arrow_table().schema
        except Exception as _:
            return None

    def __save_into_disk(self, data: Iterable[pa.Table]):
        """Consumes the entire iterable and writes it to DuckDB."""
        logger.info(f"Saving data into disk at {self.db_path} to {self.table_name}")
        with duckdb.connect(self.db_path) as connection:
            # We create the table with the first batch to define schema
            try:
                data_batch = next(data)
                connection.register("data_batch", data_batch)
                connection.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM data_batch")
                connection.unregister("data_batch")
            except StopIteration:
                logger.warning("Stream was empty. Nothing to save.")
                return
            for data_batch in data:
                connection.register("data_batch", data_batch)
                connection.execute(f"INSERT INTO {self.table_name} SELECT * FROM data_batch")
                connection.unregister("data_batch")
        logger.info(f"Successfully stored stream to {self.db_path}")

    def save_to_disk(self, data: Iterable[pa.Table]) -> bool:
        self.__save_into_disk(data=data)
        return True

    def save_to_disk_and_load(self, data: Iterable[pa.Table]) -> Iterable[pa.Table]:
        # when we save into disk, we consume the iterator, so we need to reload the data from duckdb
        self.__save_into_disk(data=data)
        return self.get_data()

    def get_data(self, columns_filter: Optional[list[str]] = None) -> Iterable[pa.Table]:
        """
        Fetches data from DuckDB.
        """
        select_clause = "*"
        if columns_filter:
            # Ensure we only select columns that actually exist in the DB
            valid_cols = [c for c in columns_filter if c in self.get_schema().names]
            select_clause = ", ".join([f'"{c}"' for c in valid_cols])

        logger.info(f"Loading data from disk at {self.db_path}")
        with duckdb.connect(self.db_path) as conn:
            # DuckDB's fetch_record_batch_reader is great for large datasets
            # as it streams the results rather than loading the whole table at once.
            reader = conn.execute(f"SELECT {select_clause} FROM {self.table_name}").fetch_record_batch(
                self.batch_size
            )
            for batch in reader:
                # Convert RecordBatch to Table for consistency with your previous code
                yield pa.Table.from_batches([batch])

if __name__ == '__main__':
    from data_integration_pipeline.settings import TEMP
    db_path = os.path.join(TEMP, 'audits', 'duckdb', 'audit.db')
    table_name = 'audit_business_entity_registry_silver'
    duckdb_client = DuckdbClient(db_path=db_path, table_name=table_name)
    data = duckdb_client.get_data()
    for batch in data:
        print(batch)