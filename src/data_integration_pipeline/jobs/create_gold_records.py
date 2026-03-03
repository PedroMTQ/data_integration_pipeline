from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET
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

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, metadata: dict) -> str:
        run_metadata = SplinkRunMetadata(**metadata)
        logger.info(f"Processing {run_metadata.deduplicated_records_s3_path} to create data mart records")
        processor = GoldRecordsProcessor(data_model=GoldRecord)
        view_name = processor.process_data(s3_path=run_metadata.deduplicated_records_s3_path)
        return view_name

    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        for s3_path in self.s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r"metadata\.json"):
            metadata_dict = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET).read_json()
            metadata = SplinkRunMetadata(**metadata_dict)
            # we check if we ran the integrated records generation
            if self.s3_client.file_exists(s3_path=metadata.deduplicated_records_s3_path):
                metadata_list.append(metadata)
        for run_metadata in get_latest_metadata_by_table_group(metadata_list=metadata_list):
            yield run_metadata.to_dict()

    def run(self) -> str:
        for metadata in self.get_data_to_process():
            self.process_data(metadata)

def process_task(metadata: dict):
    job = CreateGoldRecords()
    return job.process_data(metadata)

def get_tasks() -> list[dict]:
    job = CreateGoldRecords()
    return list(job.get_data_to_process())


if __name__ == "__main__":
    job = CreateGoldRecords()
    job.run()
