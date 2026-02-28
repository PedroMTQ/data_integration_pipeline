from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET, TEMP
import os
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.core.entity_resolution.links_processor import LinksProcessor
from datetime import datetime
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.core.entity_resolution.integrated_record import Record
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_writer import S3FileWriter


class CreateIntegratedRecords:
    """
    Processes links and merges all data to create business-drivsen data
    """
    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, metadata: SplinkRunMetadata) -> str:
        logger.info(f'Processing {metadata}')
        db_path = os.path.join(TEMP, ENTITY_RESOLUTION_DATA_FOLDER, metadata.run_id, "records.duckdb")
        processor = LinksProcessor(metadata=metadata, bucket_name=DATA_BUCKET, db_path=db_path)
        errors_s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, metadata.run_id, 'errors.parquet')
        integrated_records_s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, metadata.run_id, 'integrated_records.parquet')
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)
        integrated_records_writer = S3FileWriter(s3_path=integrated_records_s3_path, bucket_name=DATA_BUCKET)
        with integrated_records_writer as writer, fail_writer as errors_writer:
            for cluster in processor.run():
                cluster_dict = {"data": cluster}
                try:
                    record = Record(**cluster_dict)
                    writer.write_row(record.model_dump())
                except Exception as e:
                    # TODO change to error when we have better datas
                    logger.debug(f'Error processing integrated record: {cluster} due to  {e}')
                    errors_writer.write_row(cluster_dict)

    @staticmethod
    def get_latest_metadata_by_table_group(metadata_list: list["SplinkRunMetadata"]) -> dict[str, "SplinkRunMetadata"]:
        """
        Groups runs by their input table combinations and returns only
        the most recent SplinkRunMetadata object for each group.
        """
        latest_runs: dict[str, SplinkRunMetadata] = {}
        for entry in metadata_list:
            # 1. Access attributes directly from the object
            tables = entry.inputs.get("table_names", [])
            table_group = ",".join(sorted(tables))
            # 2. Parse the timestamp string into a datetime for comparison
            # We assume entry.timestamp is an ISO format string
            current_ts = datetime.fromisoformat(entry.timestamp)

            if table_group not in latest_runs:
                # First time seeing this group? It's the current winner.
                latest_runs[table_group] = entry
            else:
                # Compare against the existing winner's timestamp
                existing_ts = datetime.fromisoformat(latest_runs[table_group].timestamp)
                if current_ts > existing_ts:
                    latest_runs[table_group] = entry
        return list(latest_runs.values())

    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        for s3_path in self.s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r"metadata\.json"):
            metadata = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET).read_json()
            metadata_list.append(SplinkRunMetadata(**metadata))
        target_runs = self.get_latest_metadata_by_table_group(metadata_list=metadata_list)
        return target_runs

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        list_run_metadata: list[SplinkRunMetadata] = self.get_data_to_process()
        for run_metadata in list_run_metadata:
            return self.process_data(run_metadata)


# def process_task(table_names: list[dict]):
#     job = EntityResolutionJob()
#     silver_s3_path = job.process_data(table_names)
#     return silver_s3_path


# def get_tasks() -> list[dict]:
#     job = EntityResolutionJob()
#     return job.get_data_to_process()


if __name__ == "__main__":
    job = CreateIntegratedRecords()
    print(job.run())
