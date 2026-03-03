from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import ENTITY_RESOLUTION_DATA_FOLDER, DATA_BUCKET, TEMP
import os
from collections import Counter
from data_integration_pipeline.io.file_reader import S3FileReader
from data_integration_pipeline.core.entity_resolution.links_processor import LinksProcessor
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.core.entity_resolution.integrated_record import Record
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.file_writer import S3FileWriter
from data_integration_pipeline.core.metrics import Metrics
from data_integration_pipeline.core.utils import get_latest_metadata_by_table_group


class CreateIntegratedRecords:
    def process_data(self, metadata: dict) -> str:
        run_metadata = SplinkRunMetadata(**metadata)
        logger.info(f"Processing {run_metadata}")
        db_path = os.path.join(TEMP, ENTITY_RESOLUTION_DATA_FOLDER, run_metadata.run_id, "records.duckdb")
        processor = LinksProcessor(metadata=run_metadata, bucket_name=DATA_BUCKET, db_path=db_path)
        errors_s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, run_metadata.run_id, "errors.parquet")
        integrated_records_s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, run_metadata.run_id, "integrated_records.parquet")
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)
        integrated_records_writer = S3FileWriter(s3_path=integrated_records_s3_path, bucket_name=DATA_BUCKET)
        metrics = Metrics()
        records_to_alt_entities_counter = Counter()
        with integrated_records_writer as writer, fail_writer as errors_writer:
            for cluster in processor.run():
                record = None
                if not cluster:
                    continue
                try:
                    record = Record.from_cluster(cluster)
                    records_to_alt_entities_counter[len(record.alt_entities)] += 1
                    writer.write_row(record.model_dump())
                    metrics.log_result(is_success=True)
                except Exception as e:
                    # TODO change to error when we have better datas
                    logger.debug(f"Error processing integrated record: {record} with {len(cluster)} records in cluster {cluster} due to  {e}")
                    cluster_dict = {"cluster_id": [i.get("cluster_id") for i in cluster][0], "data": cluster}
                    errors_writer.write_row(cluster_dict)
                    metrics.log_result(is_success=False)
        logger.info(f"Processed {run_metadata.run_id} and wrote output to {integrated_records_s3_path}. File metrics: {metrics}")
        logger.info(f"Records to alt entities counter: {records_to_alt_entities_counter}")
        if metrics.failures:
            logger.warning(f"Wrote {metrics.failures} invalid rows to {errors_s3_path}")
        return integrated_records_s3_path

    def get_data_to_process(self) -> list[dict]:
        metadata_list = []
        s3_client = S3Client()
        for s3_path in s3_client.get_files(prefix=ENTITY_RESOLUTION_DATA_FOLDER, file_name_pattern=r"metadata\.json"):
            metadata = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET).read_json()
            metadata_list.append(SplinkRunMetadata(**metadata))
        run_metadata: SplinkRunMetadata
        for run_metadata in  get_latest_metadata_by_table_group(metadata_list=metadata_list):
            yield run_metadata.to_dict()

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        list_run_metadata: list[dict] = self.get_data_to_process()
        for run_metadata in list_run_metadata:
            self.process_data(run_metadata)


def process_task(metadata: dict):
    job = CreateIntegratedRecords()
    integrated_records_s3_path = job.process_data(metadata)
    return integrated_records_s3_path

def get_tasks() -> list[dict]:
    job = CreateIntegratedRecords()
    return list(job.get_data_to_process())

if __name__ == "__main__":
    job = CreateIntegratedRecords()
    job.run()
