
import great_expectations as gx
from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import SILVER_DATA_FOLDER,DATA_BUCKET
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.core.data_vault.record_to_vault_converter import RecordToVaultConverter, VaultedRecordPayload
from data_integration_pipeline.io.file_reader import S3FileReader


class LoadSilverDataJob:
    def __init__(self):
        self.s3_client = S3Client(bucket_name=DATA_BUCKET)

    def _add_to_buffer(self, batch_buffer: dict, vault_payload: VaultedRecordPayload):
        """
        Helper to safely append records to the batch buffer.
        """
        for table_type, table in vault_payload.items():
            for table_name, records in table.items():
                batch_buffer[table_type][table_name].extend(records)

    def run(self) -> bool:
        for silver_s3_path in self.s3_client.get_files(prefix=SILVER_DATA_FOLDER):
            data_model = ModelMapper.get_data_model(silver_s3_path)
            reader = S3FileReader(s3_path=silver_s3_path, bucket_name=DATA_BUCKET)
            with reader as stream_in:
                for row in stream_in:
                    record = data_model(**row)
                    # this decomposes a record into a data vault record based on the vault record schema and respective annotationsss
                    vault_payload = RecordToVaultConverter(record).run()
                    self._add_to_buffer(batch_buffer=batch_buffer, vault_payload=vault_payload)
                    return
            pass


if __name__ == "__main__":
    job = LoadSilverDataJob()
    job.run()