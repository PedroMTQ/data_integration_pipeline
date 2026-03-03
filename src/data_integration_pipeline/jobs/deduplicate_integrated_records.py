from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET
from data_integration_pipeline.io.file_reader import S3FileReader

from data_integration_pipeline.core.entity_resolution.duplicates_processor import DuplicatesProcessor
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.core.utils import get_latest_metadata_by_table_group


class DeduplicateIntegratedRecordsJob:
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, metadata: dict) -> str:
        run_metadata = SplinkRunMetadata(**metadata)
        logger.info(f"Processing {run_metadata.integrated_records_s3_path} to create deduplicated records data")
        processor = DuplicatesProcessor(partition_by_keys=["anchor_entity.entity_id", "anchor_entity.data_source"])
        processor.run(
            input_path=run_metadata.integrated_records_s3_path, output_path=run_metadata.deduplicated_records_s3_path, data_type="integrated"
        )
        return run_metadata.deduplicated_records_s3_path

    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        for s3_path in self.s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r"metadata\.json"):
            metadata_dict = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET).read_json()
            metadata = SplinkRunMetadata(**metadata_dict)
            if self.s3_client.file_exists(s3_path=metadata.integrated_records_s3_path):
                metadata_list.append(metadata)
        for run_metadata in get_latest_metadata_by_table_group(metadata_list=metadata_list):
            yield run_metadata.to_dict()

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        for metadata in self.get_data_to_process():
            self.process_data(metadata)


def process_task(metadata: dict):
    job = DeduplicateIntegratedRecordsJob()
    return job.process_data(metadata)


def get_tasks() -> list[dict]:
    job = DeduplicateIntegratedRecordsJob()
    return list(job.get_data_to_process())


if __name__ == "__main__":
    job = DeduplicateIntegratedRecordsJob()
    job.run()
