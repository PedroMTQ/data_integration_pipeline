from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.io.file_reader import S3FileReader
from typing import Iterable

from data_integration_pipeline.core.entity_resolution.duplicates_processor import DuplicatesProcessor
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.core.utils import get_latest_metadata_by_table_group
from pathlib import Path
from data_integration_pipeline.settings import SILVER_DATA_FOLDER, PARQUET_TABLE_SUFFIX


class DeduplicateSilverDataJob:
    """
    Processes links and merges all data to create deduplicated records data
    """

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, silver_s3_path: str, deduplicated_s3_path: str) -> str:
        logger.info(f"Processing {silver_s3_path} to create deduplicated records data")
        data_model = ModelMapper().get_data_model(silver_s3_path)
        processor = DuplicatesProcessor()
        processor.run(input_path=silver_s3_path, output_path=deduplicated_s3_path, partition_by=data_model._primary_key)

    def get_data_to_process(self) -> Iterable[dict]:
        res = []
        for silver_s3_path in self.s3_client.get_delta_tables(prefix="silver"):
            path_obj = Path(silver_s3_path)
            path_suffix = path_obj.relative_to(SILVER_DATA_FOLDER)
            deduplicated_s3_path = str(Path(SILVER_DATA_FOLDER) / path_suffix.with_name(f'deduplicated{PARQUET_TABLE_SUFFIX}'))
            yield {
                "silver_s3_path": silver_s3_path,
                "deduplicated_s3_path": deduplicated_s3_path,
            }
        return res


    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        for task in self.get_data_to_process():
            self.process_data(**task)



# def process_task(table_names: list[dict]):
#     job = EntityResolutionJob()
#     silver_s3_path = job.process_data(table_names)
#     return silver_s3_path


# def get_tasks() -> list[dict]:
#     job = EntityResolutionJob()
#     return job.get_data_to_process()


if __name__ == "__main__":
    job = DeduplicateSilverDataJob()
    print(job.run())
