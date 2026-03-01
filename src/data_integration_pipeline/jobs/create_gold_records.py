from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET, DELTA_TABLE_SUFFIX, DATA_MART_DATA_FOLDER
import os
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.core.data_mart.gold_records_processor import GoldRecordsProcessor
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.core.entity_resolution.integrated_record import Record
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.core.metrics import Metrics
from data_integration_pipeline.core.utils import get_latest_metadata_by_table_group
import pyarrow as pa

class CreateGoldRecords:
    """
    Processes links and merges all data to create integrated records data
    """
    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, metadata: SplinkRunMetadata) -> str:
        logger.info(f'Processing {metadata}')
        data_model = ModelMapper.get_data_model(metadata.integrated_records_s3_path)
        processor = GoldRecordsProcessor(data_model=data_model)
        view_name = processor.process_data(s3_path=metadata.integrated_records_s3_path)
        return view_name



    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        for s3_path in self.s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r"metadata\.json"):
            metadata_dict = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET).read_json()
            metadata = SplinkRunMetadata(**metadata_dict)
            # we check if we ran the integrated records generation
            if self.s3_client.file_exists(s3_path=metadata.integrated_records_s3_path):
                metadata_list.append(metadata)
        target_runs = get_latest_metadata_by_table_group(metadata_list=metadata_list)
        return target_runs

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        list_run_metadata: list[SplinkRunMetadata] = self.get_data_to_process()
        for run_metadata in list_run_metadata:
            view_name = self.process_data(run_metadata)
            print(f'Created view {view_name} for {run_metadata.integrated_records_s3_path}')

# def process_task(table_names: list[dict]):
#     job = EntityResolutionJob()
#     silver_s3_path = job.process_data(table_names)
#     return silver_s3_path


# def get_tasks() -> list[dict]:
#     job = EntityResolutionJob()
#     return job.get_data_to_process()


if __name__ == "__main__":
    job = CreateGoldRecords()
    print(job.run())
