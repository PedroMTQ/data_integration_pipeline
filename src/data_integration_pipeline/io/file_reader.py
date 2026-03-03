import csv
from typing import Iterable, Union
import json
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import s3fs
from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
    DATA_BUCKET,
    PARQUET_TABLE_SUFFIX,
)
from data_integration_pipeline.io.logger import logger
from pyarrow import csv as pa_csv


class FileReader:
    _file_handle = None
    _file_path = None
    _generator = None

    def __init__(self, as_table: bool = False):
        self.as_table = as_table

    def __iter__(self) -> Iterable[dict]:
        """
        Determines the file type and yields rows one by one.
        """
        extension = Path(self._file_path).suffix.lower()
        if extension == ".csv":
            self._generator = self._read_csv()
        elif extension == PARQUET_TABLE_SUFFIX:
            self._generator = self._read_parquet()
        elif extension == ".json":
            self._generator = self._read_json()
        else:
            raise ValueError(f"Unsupported file extension: {extension}")
        return self._generator

    def __next__(self):
        # This allows next(reader) to work
        if self._generator is None:
            iter(self)  # Initialize the generator
        return next(self._generator)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"Finished reading from {self._file_path}")
        # If your reader has an internal file handle, close it here
        if hasattr(self, "fs") and hasattr(self, "_file_handle"):
            self._file_handle.close()

    def _read_json(self) -> Iterable[dict]:
        """
        Reads a JSON file and yields the content.
        If the JSON is a list, it yields items. If a dict, it yields the dict.
        """
        # Note: We rely on the child classes to set _file_handle correctly
        content = json.load(self._file_handle)
        if isinstance(content, list):
            for item in content:
                yield item
        else:
            yield content

    # TODO this is a bit non-sensical, but this way we keep the same interface, maybe change it later?
    def read_json(self) -> dict:
        return next(self)


class LocalFileReader(FileReader):
    def __init__(self, file_path: str, as_table: bool = False):
        super().__init__(as_table=as_table)
        self._file_path = file_path
        if not Path(file_path).exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

    def _read_csv(self) -> Iterable[Union[dict, pa.Table]]:
        if self.as_table:
            # Binary mode is better for pyarrow's native CSV reader
            self._file_handle = open(self._file_path, mode="rb")
            reader = pa_csv.open_csv(self._file_handle)
            for batch in reader:
                yield pa.Table.from_batches([batch])
        else:
            self._file_handle = open(self._file_path, mode="r", encoding="utf-8")
            reader = csv.DictReader(self._file_handle)
            for row in reader:
                yield dict(row)

    def _read_parquet(self) -> Iterable[Union[dict, pa.Table]]:
        self._file_handle = open(self._file_path, mode="rb")
        parquet_file = pq.ParquetFile(self._file_handle)
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            if self.as_table:
                yield table
            else:
                for row in table.to_pylist():
                    yield row

    def _read_json(self) -> Iterable[dict]:
        self._file_handle = open(self._file_path, mode="r", encoding="utf-8")
        return super()._read_json()


class S3FileReader(FileReader):
    def __init__(
        self,
        s3_path: str,
        bucket_name: str,
        as_table: bool = False,
        aws_access_key: str = S3_ACCESS_KEY,
        aws_secret_access_key: str = S3_SECRET_ACCESS_KEY,
        s3_endpoint_url: str = S3_ENDPOINT_URL,
    ):
        super().__init__(as_table=as_table)
        self.bucket_name = bucket_name
        if not s3_path.startswith(f"s3://{self.bucket_name}/"):
            s3_path = f"s3://{self.bucket_name}/{s3_path}"
        self._file_path = s3_path
        # Initialize s3fs with your specific credentials/endpoint
        self.fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_access_key, client_kwargs={"endpoint_url": s3_endpoint_url})

    def _read_csv(self) -> Iterable[Union[dict, pa.Table]]:
        if self.as_table:
            self._file_handle = self.fs.open(self._file_path, mode="rb")
            reader = pa_csv.open_csv(self._file_handle)
            for batch in reader:
                yield pa.Table.from_batches([batch])
        else:
            self._file_handle = self.fs.open(self._file_path, mode="rt", encoding="utf-8")
            reader = csv.DictReader(self._file_handle)
            for row in reader:
                yield dict(row)

    def _read_parquet(self) -> Iterable[Union[dict, pa.Table]]:
        self._file_handle = self.fs.open(self._file_path, mode="rb")
        parquet_file = pq.ParquetFile(self._file_handle)
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            if self.as_table:
                yield table
            else:
                for row in table.to_pylist():
                    yield row

    def _read_json(self) -> Iterable[dict]:
        # s3fs handles the text stream for json.load perfectly
        self._file_handle = self.fs.open(self._file_path, mode="rt", encoding="utf-8")
        return super()._read_json()


if __name__ == "__main__":
    # file_path = "/home/pedroq/workspace/data_integration_pipeline/tests/data/business_entity_registry.csv"
    # for table in LocalFileReader(file_path, as_table=True):
    #     print(table.shape)
    # for row in LocalFileReader(file_path, as_table=False):
    #     print(row)
    #     break
    s3_path = "silver/sub_contractors_registry/deduplicated.parquet"
    for table in S3FileReader(s3_path, bucket_name=DATA_BUCKET, as_table=True):
        print(table)
    # s3_path = "entity_resolution/019ca4b0-2e7d-7308-b854-d81e5533bd5b/metadata.json"
    # data = S3FileReader(s3_path, bucket_name=DATA_BUCKET).read_json()
    # print(data)
