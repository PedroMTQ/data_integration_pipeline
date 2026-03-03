from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from typing import Iterable

from data_integration_pipeline.core.entity_resolution.duplicates_processor import DuplicatesProcessor
from data_integration_pipeline.io.logger import logger
from pathlib import Path
from data_integration_pipeline.settings import SILVER_DATA_FOLDER, PARQUET_TABLE_SUFFIX


class DeduplicateSilverDataJob:
    def process_data(self, silver_s3_path: str, deduplicated_s3_path: str) -> str:
        logger.info(f"Processing {silver_s3_path} to create deduplicated records data")
        data_model = ModelMapper().get_data_model(silver_s3_path)
        processor = DuplicatesProcessor(partition_by_keys=[data_model._primary_key])
        return processor.run(input_path=silver_s3_path, output_path=deduplicated_s3_path, data_type="silver")

    def get_data_to_process(self) -> Iterable[dict]:
        s3_client = S3Client()
        for silver_s3_path in s3_client.get_delta_tables(prefix="silver"):
            path_obj = Path(silver_s3_path)
            path_suffix = path_obj.relative_to(SILVER_DATA_FOLDER)
            deduplicated_s3_path = str(Path(SILVER_DATA_FOLDER) / path_suffix.with_name(f"deduplicated{PARQUET_TABLE_SUFFIX}"))
            yield {
                "silver_s3_path": silver_s3_path,
                "deduplicated_s3_path": deduplicated_s3_path,
            }

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        for task in self.get_data_to_process():
            self.process_data(**task)


def process_task(task_dict: dict):
    job = DeduplicateSilverDataJob()
    return job.process_data(**task_dict)


def get_tasks() -> list[dict]:
    job = DeduplicateSilverDataJob()
    return list(job.get_data_to_process())


if __name__ == "__main__":
    job = DeduplicateSilverDataJob()
    job.run()
