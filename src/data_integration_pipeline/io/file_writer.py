import csv
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import s3fs
from typing import Any
from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
    DATA_BUCKET,
)
from data_integration_pipeline.io.logger import logger
import pyarrow.csv as pa_csv


class FileWriter:
    _file_handle = None
    _file_path = None

    def __init__(self, file_path: str, chunk_size: int = 1000):
        self._file_path = file_path
        self.chunk_size = chunk_size
        self.buffer: list[dict[str, Any]] = []
        self._writer = None
        self.extension = Path(self._file_path).suffix.lower()

    def write_table(self, table: pa.Table):
        """Writes a pyarrow Table directly to the file."""
        # We flush the existing row buffer first to maintain order
        if self.buffer:
            self.__flush()
        if self.extension == ".csv":
            self._write_csv_table(table)
        elif self.extension == ".parquet":
            self._write_parquet_table(table)

    def write_row(self, row: dict[str, Any]):
        """Adds a row to the buffer and flushes if chunk_size is reached."""
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.__flush()

    def __flush(self):
        """Dumps buffer to the destination."""
        if not self.buffer:
            return
        if self.extension == ".csv":
            self._write_csv_chunk()
        elif self.extension == ".parquet":
            self._write_parquet_chunk()

        self.buffer = []

    def __close(self):
        """Finalizes the file and closes handles."""
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


class LocalFileWriter(FileWriter):
    def _write_csv_chunk(self):
        mode = "a" if self._file_handle else "w"
        if not self._file_handle:
            self._file_handle = open(self._file_path, mode=mode, newline="", encoding="utf-8")
            self._writer = csv.dictWriter(self._file_handle, fieldnames=self.buffer[0].keys())
            self._writer.writeheader()
        self._writer.writerows(self.buffer)

    def _write_parquet_chunk(self):
        table = pa.Table.from_pylist(self.buffer)
        if not self._writer:
            self._writer = pq.ParquetWriter(self._file_path, table.schema, compression="snappy")
        self._writer.write_table(table)

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
        super().__init__(s3_path, chunk_size)
        self.bucket_name = bucket_name
        if not s3_path.startswith(f"s3://{self.bucket_name}/"):
            s3_path = f"s3://{self.bucket_name}/{s3_path}"
        self._file_path = s3_path

        self.fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_access_key, client_kwargs={"endpoint_url": s3_endpoint_url})

    def _write_csv_chunk(self):
        if not self._file_handle:
            # We open in 'wt' mode. s3fs handles the multi-part upload/append logic.
            self._file_handle = self.fs.open(self._file_path, mode="wt", encoding="utf-8")
            self._writer = csv.dictWriter(self._file_handle, fieldnames=self.buffer[0].keys())
            self._writer.writeheader()
        self._writer.writerows(self.buffer)

    def _write_csv_table(self, table: pa.Table):
        if not self._file_handle:
            self._file_handle = self.fs.open(self._file_path, mode="wb")
            # Standard pyarrow CSV writer doesn't support easy "appending" to a headered stream
            # as easily as DictWriter, so we use it for a one-shot or handle with care
            pa_csv.write_csv(table, self._file_handle)
        else:
            # If already open, we'd need to handle the header differently
            pa_csv.write_csv(table, self._file_handle, write_options=pa_csv.WriteOptions(include_header=False))

    def _write_parquet_chunk(self):
        table = pa.Table.from_pylist(self.buffer)
        if not self._writer:
            # For Parquet on S3, we pass the s3fs file handle to the ParquetWriter
            self._file_handle = self.fs.open(self._file_path, mode="wb")
            self._writer = pq.ParquetWriter(self._file_handle, table.schema, compression="snappy")
        self._writer.write_table(table)

    def _write_parquet_table(self, table: pa.Table):
        if not self._writer:
            self._file_handle = self.fs.open(self._file_path, mode="wb")
            self._writer = pq.ParquetWriter(self._file_handle, table.schema, compression="snappy")
        self._writer.write_table(table)


if __name__ == "__main__":
    target_path = "registry_processed.parquet"
    with LocalFileWriter(target_path, chunk_size=2000) as writer:
        writer.write_row({"test": 1})
    target_path = "registry_processed.parquet"
    with S3FileWriter(target_path, bucket_name=DATA_BUCKET, chunk_size=2000) as writer:
        writer.write_row({"test": 1})

    with S3FileWriter("output.parquet", bucket_name=DATA_BUCKET) as writer:
        # Small single row
        writer.write_row({"id": 1, "data": "start"})

        # Large bulk table from another process/reader
        my_table = pa.Table.from_pylist([{"id": i, "data": "bulk"} for i in range(2, 100)])
        writer.write_table(my_table)
