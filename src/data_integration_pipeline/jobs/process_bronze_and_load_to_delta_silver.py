from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import (
    DATA_BUCKET,
    BRONZE_DATA_FOLDER,
    SILVER_DATA_FOLDER,
    ARCHIVE_DATA_FOLDER,
    PROCESSING_ERRORS_DATA_FOLDER,
    DELTA_CLIENT_BATCH_SIZE,
    DELTA_TABLE_SUFFIX,
    PARQUET_TABLE_SUFFIX,
)
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper, BaseRecordType
from pathlib import Path
from data_integration_pipeline.io.logger import logger
from typing import Iterable
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.io.file_writer import S3FileWriter
from data_integration_pipeline.io.delta_client import DeltaClient
import pyarrow as pa


class ProcessBronzetoSilver:
    """
    Processes data from bronze to silver, processes one file at a time to allow for parallelism of tasks
    """

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def _flush_buffer(self, writer: DeltaClient, data: list[dict], s3_path: str, data_model: BaseRecordType):
        """Converts the list of dicts to a PyArrow table and writes to Delta."""
        if not data:
            return
        writer.write(
            s3_path=s3_path,
            data=pa.Table.from_pylist(data, schema=data_model._pa_schema),
            primary_key=data_model._primary_key,
            partition_key=data_model._partition_key,
        )

    def process_data(self, bronze_s3_path: str, archive_s3_path: str, silver_s3_path: str, errors_s3_path: str):
        logger.info(f"Reading raw data from {bronze_s3_path} and writing processed data to {silver_s3_path}")
        reader = S3FileReader(s3_path=bronze_s3_path, bucket_name=DATA_BUCKET)
        writer = DeltaClient()
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)
        data_model = ModelMapper.get_data_model(bronze_s3_path)
        # you'd want to expose these metrics to a dashboard e.g., using prometheus+grafana or send events somewhere
        metrics = {"success": 0, "failures": 0, "total": 0}
        batch_buffer = []
        with reader as stream_in, fail_writer as fail_stream_out:
            for row in stream_in:
                metrics["total"] += 1
                try:
                    processed_data = data_model(**row)
                    batch_buffer.append(processed_data.model_dump())
                    metrics["success"] += 1
                except Exception as e:
                    logger.warning(f"Validation error: {e}")
                    fail_stream_out.write_row(row)
                    metrics["failures"] += 1
                # Check if we should flush the buffer
                if len(batch_buffer) >= DELTA_CLIENT_BATCH_SIZE:
                    self._flush_buffer(
                        writer=writer,
                        batch_buffer=batch_buffer,
                        data_model=data_model,
                        s3_path=silver_s3_path,
                    )
                    batch_buffer = []  # Reset buffer
            # Final flush for any remaining records in the buffer
            if batch_buffer:
                self._flush_buffer(
                    writer=writer,
                    data=batch_buffer,
                    data_model=data_model,
                    s3_path=silver_s3_path,
                )
        logger.info(f"Processed {bronze_s3_path} and wrote output to {silver_s3_path}. File metrics: {metrics}")
        if metrics["failures"]:
            logger.warning(f"Wrote {metrics['failures']} invalid rows to {errors_s3_path}")
        self.s3_client.move_file(current_path=bronze_s3_path, new_path=archive_s3_path)
        return silver_s3_path

    def get_data_to_process(self) -> Iterable[dict]:
        # this could be distributed processing, but let's keep it simple
        for bronze_s3_path in self.s3_client.get_files(prefix=BRONZE_DATA_FOLDER):
            path_obj = Path(bronze_s3_path)
            try:
                path_suffix = path_obj.relative_to(BRONZE_DATA_FOLDER)
            except Exception:
                logger.error(f"File path is not valid from bronze processing: {bronze_s3_path}")
            silver_s3_path = str(Path(SILVER_DATA_FOLDER) / path_suffix.with_suffix(DELTA_TABLE_SUFFIX))
            errors_s3_path = str(Path(PROCESSING_ERRORS_DATA_FOLDER) / path_suffix.with_suffix(PARQUET_TABLE_SUFFIX))
            archive_s3_path = str(Path(ARCHIVE_DATA_FOLDER) / path_suffix)
            if self.s3_client.file_exists(silver_s3_path):
                logger.debug(f"{bronze_s3_path} already processed, skipping...")
                continue
            yield {
                "bronze_s3_path": bronze_s3_path,
                "archive_s3_path": archive_s3_path,
                "silver_s3_path": silver_s3_path,
                "errors_s3_path": errors_s3_path,
            }

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        for task in self.get_data_to_process():
            self.process_data(**task)


def process_task(task_dict: dict):
    job = ProcessBronzetoSilver()
    silver_s3_path = job.process_data(**task_dict)
    return silver_s3_path


def get_tasks() -> list[dict]:
    job = ProcessBronzetoSilver()
    return list(job.get_data_to_process())


if __name__ == "__main__":
    job = ProcessBronzetoSilver()
    job.run()
