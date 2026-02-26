from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import DATA_BUCKET, BRONZE_DATA_FOLDER, SILVER_DATA_FOLDER, ERRORS_DATA_FOLDER
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from pathlib import Path
from data_integration_pipeline.io.logger import logger
from pydantic import BaseModel
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.io.file_writer import S3FileWriter


class ProcessBronzetoSilver:
    """
    Processes data from bronze to silver, processes one file at a time to allow for parallelism of tasks
    """

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client(bucket_name=DATA_BUCKET)

    def process_data(self, bronze_s3_path: str, silver_s3_path: str, errors_s3_path: str):
        reader = S3FileReader(s3_path=bronze_s3_path, bucket_name=DATA_BUCKET)
        writer = S3FileWriter(s3_path=silver_s3_path, bucket_name=DATA_BUCKET)
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)
        data_model: BaseModel = ModelMapper.get_data_model(bronze_s3_path)
        # you'd want to expose these metrics to a dashboard e.g., using prometheus+grafana or send events somewhere
        metrics = {"success": 0, "failures": 0, "total": 0}
        with reader as stream_in, writer as stream_out, fail_writer as fail_stream_out:
            for row in stream_in:
                serialized_data = None
                try:
                    processed_data = data_model(**row)
                    serialized_data = processed_data.model_dump()
                except Exception as e:
                    logger.warning(f"Validation error in {bronze_s3_path}: {e}")
                    fail_stream_out.write_row(row)
                if serialized_data:
                    stream_out.write_row(serialized_data)
                    metrics["success"] += 1
                else:
                    metrics["failures"] += 1
                metrics["total"] += 1
        logger.info(f"Processed {bronze_s3_path} and wrote output to {silver_s3_path}. File metrics: {metrics}")
        if metrics['failures']:
            logger.warning(f'Wrote {metrics["failures"]} invalid rows to {errors_s3_path}')

    def get_data_to_process(self) -> dict:
        # this could be distributed processing, but let's keep it simple
        for bronze_s3_path in self.s3_client.get_files(prefix=BRONZE_DATA_FOLDER):
            path_obj = Path(bronze_s3_path)
            try:
                path_suffix = path_obj.relative_to(BRONZE_DATA_FOLDER)
            except Exception:
                logger.error(f"File path is not valid from bronze processing: {bronze_s3_path}")
            silver_s3_path = str(Path(SILVER_DATA_FOLDER) / path_suffix.with_suffix(".parquet"))
            errors_s3_path = str(Path(ERRORS_DATA_FOLDER) / path_suffix.with_suffix(".parquet"))
            if self.s3_client.file_exists(silver_s3_path):
                logger.debug(f"{bronze_s3_path} already processed, skipping...")
                continue
            return {
                "bronze_s3_path": bronze_s3_path,
                "silver_s3_path": silver_s3_path,
                "errors_s3_path": errors_s3_path,
            }
        logger.debug("No data to process")

    def run(self):
        task: dict = self.get_data_to_process()
        if task:
            self.process_data(**task)


if __name__ == "__main__":
    job = ProcessBronzetoSilver()
    job.run()
