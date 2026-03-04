from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER
from data_integration_pipeline.core.data_processing.model_mapper import GoldRecord
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.core.data_mart.gold_records_processor import GoldRecordsProcessor
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.core.utils import get_latest_metadata_by_table_group


class CreateGoldRecords:
    """
    Processes links and merges all data to create integrated records data
    """

    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, metadata: dict) -> str:
        run_metadata = SplinkRunMetadata(**metadata)
        logger.info(f'Processing {run_metadata.deduplicated_records_s3_path} to create data mart records')
        processor = GoldRecordsProcessor(data_model=GoldRecord)
        delta_tables = processor.process_data(s3_path=run_metadata.deduplicated_records_s3_path)
        return delta_tables

    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        for s3_path in self.s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r'metadata\.json'):
            metadata_dict = S3FileReader(s3_path=s3_path).read_json()
            metadata = SplinkRunMetadata(**metadata_dict)
            # we check if we ran the integrated records generation
            if self.s3_client.file_exists(s3_path=metadata.deduplicated_records_s3_path):
                metadata_list.append(metadata)
        for run_metadata in get_latest_metadata_by_table_group(metadata_list=metadata_list):
            yield run_metadata.to_dict()

    def run(self) -> str:
        from data_integration_pipeline.io.delta_client import DeltaClient

        delta_client = DeltaClient()
        for metadata in self.get_data_to_process():
            delta_tables = self.process_data(metadata)
            print('-' * 30)
            print(SplinkRunMetadata(**metadata))
            print('-' * 30)
            for table_name, delta_table_path in delta_tables.items():
                table = delta_client.read_table(delta_table_path)
                print('-' * 30)
                print(table_name)
                print('-' * 30)
                print(table)


def process_task(metadata: dict):
    job = CreateGoldRecords()
    return job.process_data(metadata)


def get_tasks() -> list[dict]:
    job = CreateGoldRecords()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = CreateGoldRecords()
    job.run()
