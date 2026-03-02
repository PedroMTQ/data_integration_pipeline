import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Union

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
    DATA_BUCKET,
    PARQUET_TABLE_SUFFIX,
)
from data_integration_pipeline.io.logger import logger


class FileWriter(ABC):
    def __init__(self, file_path: str, chunk_size: int = 1000):
        self._file_path = file_path
        self.chunk_size = chunk_size
        self.buffer: list[dict[str, Any]] = []
        self._writer = None
        self._file_handle = None

        self.extension = Path(self._file_path).suffix.lower()

        # We allow .json specifically for the S3 write_json utility
        valid_extensions = {PARQUET_TABLE_SUFFIX, ".json"}
        if self.extension not in valid_extensions:
            raise ValueError(f"Unsupported file extension: {self.extension}")

    def write_table(self, table: Union[pa.Table, pa.RecordBatch]):
        """Writes a pyarrow Table (or RecordBatch) directly to the file."""
        if self.extension == ".json":
            raise TypeError("write_table is not supported for JSON files.")

        # Intercept RecordBatches to prevent downstream crashes
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        if self.buffer:
            self.__flush()
        self._write_parquet_table(table)

    def write_row(self, row: dict[str, Any]):
        """Adds a row to the buffer and flushes if chunk_size is reached."""
        if self.extension == ".json":
            raise TypeError("write_row is not supported for JSON files. Use write_json().")

        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.__flush()

    def __flush(self):
        """Dumps buffer to the destination."""
        if not self.buffer:
            return

        table = pa.Table.from_pylist(self.buffer)
        self._write_parquet_table(table)
        self.buffer = []

    def __close(self):
        """Finalizes the file and closes handles."""
        # Only flush if we are doing buffered writing (not JSON)
        if self.extension != ".json":
            self.__flush()

        if self._writer and hasattr(self._writer, "close"):
            self._writer.close()
        if self._file_handle and hasattr(self._file_handle, "close"):
            self._file_handle.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"Finished writing to {self._file_path}")
        self.__close()

    @abstractmethod
    def _write_parquet_table(self, table: pa.Table):
        """Must be implemented by subclasses."""
        pass


class LocalFileWriter(FileWriter):
    def __init__(self, file_path: str, chunk_size: int = 1000):
        # Create directories BEFORE initializing the base class
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        super().__init__(file_path, chunk_size)

    def _write_parquet_table(self, table: pa.Table):
        if not self._writer:
            self._writer = pq.ParquetWriter(self._file_path, table.schema, compression="snappy")
        self._writer.write_table(table)


class S3FileWriter(FileWriter):
    def __init__(
        self,
        s3_path: str,
        bucket_name: str,
        chunk_size: int = 1000,
        aws_access_key: str = S3_ACCESS_KEY,
        aws_secret_access_key: str = S3_SECRET_ACCESS_KEY,
        s3_endpoint_url: str = S3_ENDPOINT_URL,
    ):
        self.bucket_name = bucket_name

        # Build the correct absolute S3 path BEFORE initializing the base class
        if not s3_path.startswith(f"s3://{self.bucket_name}/"):
            full_path = f"s3://{self.bucket_name}/{s3_path}"
        else:
            full_path = s3_path

        super().__init__(full_path, chunk_size)

        self.fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_access_key, client_kwargs={"endpoint_url": s3_endpoint_url})

    def _write_parquet_table(self, table: pa.Table):
        if not self._writer:
            self._file_handle = self.fs.open(self._file_path, mode="wb")
            self._writer = pq.ParquetWriter(self._file_handle, table.schema, compression="snappy")
        self._writer.write_table(table)

    def write_json(self, data: dict):
        """Serializes a dictionary to JSON and writes it directly to S3."""
        if self.extension != ".json":
            logger.warning(f"Writing JSON data to a file with extension '{self.extension}'")

        json_string = json.dumps(data, indent=4)
        self.fs.pipe(self._file_path, json_string.encode("utf-8"))


if __name__ == "__main__":
    target_path = "tmp/test/registry_processed.parquet"
    with LocalFileWriter(target_path, chunk_size=2000) as writer:
        writer.write_row({"test": 1})
        writer.write_table(pa.Table.from_pylist([{"test": 2}, {"test": 3}]))

    with S3FileWriter(target_path, bucket_name=DATA_BUCKET, chunk_size=2000) as writer:
        writer.write_row({"test": 1})

    with S3FileWriter("tmp/test/output.parquet", bucket_name=DATA_BUCKET) as writer:
        # Small single row
        writer.write_row({"id": 1, "data": "start"})

        # Large bulk table from another process/reader
        my_table = pa.Table.from_pylist([{"id": i, "data": "bulk"} for i in range(2, 100)])
        writer.write_table(my_table)
