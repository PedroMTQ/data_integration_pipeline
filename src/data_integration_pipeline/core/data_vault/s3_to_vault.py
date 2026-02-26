import pyarrow as pa
from typing import Iterable
from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import SILVER_DATA_FOLDER,DATA_BUCKET
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.core.data_vault.record_to_vault_converter import RecordToVaultConverter, VaultedRecordPayload
from data_integration_pipeline.io.file_reader import S3FileReader
from collections import defaultdict
from data_integration_pipeline.io.file_writer import S3FileWriter
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord


class S3ToVault():
    TABLE_TYPES_INSERT_ORDER = ['hubs', 'links', 'satellites']

    def __init__(self, data_model: BaseModel, data: Iterable[pa.Table], record_source: str):
        self.data_model = data_model
        self.data = data
        self.record_source = record_source
        self.schema = VaultSchemaGenerator(model=data_model.schema).run()

    def _add_to_buffer(self, batch_buffer: dict, vault_payload: VaultedRecordPayload):
        """
        Helper to safely append records to the batch buffer.
        """
        for table_type, table in vault_payload.items():
            for table_name, records in table.items():
                batch_buffer[table_type][table_name].extend(records)

    def get_data(self) -> Iterable[pa.Table]:
        """
        Consumes raw PyArrow tables, cleans them via Pydantic,
        and yields cleaned PyArrow tables.
        """
        data_model = ModelMapper.get_data_model(silver_s3_path)
        reader = S3FileReader(s3_path=silver_s3_path, bucket_name=DATA_BUCKET, as_table=True)
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)

        with reader as stream_in, fail_writer as fail_stream_out:
            for batch in stream_in:
                # 1. Convert Arrow Table to list of dicts
                raw_dicts = batch.to_pylist()
                batch_buffer = {i: defaultdict(list) for i in self.TABLE_TYPES_INSERT_ORDER}
                for raw_record in raw_dicts:
                    try:
                        record: BaseRecord = self.data_model(**raw_record)
                        vault_payload: VaultedRecordPayload = RecordToVaultConverter(record=record, record_source=self.record_source).run()
                        self._add_to_buffer(batch_buffer=batch_buffer, vault_payload=vault_payload)

                    # TODO in a real cleaning scenario, these would be forwarded to a dead letter queue/folder
                    except Exception as e:
                        # Log specific record failures without crashing the whole stream
                        logger.exception(f'Record {record} failed cleaning validation: {e}')
                        fail_stream_out.write_row(raw_record)
                for table_type in self.TABLE_TYPES_INSERT_ORDER:
                    for table_name, records in batch_buffer[table_type].items():
                        if not records:
                            continue
                        # Fetch the correct schema we generated earlier
                        target_schema = self.schema[table_name]
                        print(target_schema)
                        print(records[0])

                        # Create the Arrow table with strict schema enforcement
                        table = pa.Table.from_pylist(records, schema=target_schema)
                        yield table




if __name__ == "__main__":
    job = S3ToVault()
    job.run()