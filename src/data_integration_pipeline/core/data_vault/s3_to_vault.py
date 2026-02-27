import pyarrow as pa
from typing import Iterable
from data_integration_pipeline.settings import DATA_BUCKET
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.core.data_vault.record_to_vault_converter import (
    RecordToVaultConverter,
    VaultedRecordPayload,
)
from data_integration_pipeline.core.schema_converter import VaultSchemaGenerator
from data_integration_pipeline.io.file_reader import S3FileReader
from collections import defaultdict
from data_integration_pipeline.io.file_writer import S3FileWriter
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.io.logger import logger


class S3ToVault:
    TABLE_TYPES_INSERT_ORDER = ["hubs", "links", "satellites"]

    def _add_to_buffer(self, batch_buffer: dict, vault_payload: VaultedRecordPayload):
        """
        Helper to safely append records to the batch buffer.
        """
        for table_type, table in vault_payload.items():
            for table_name, records in table.items():
                batch_buffer[table_type][table_name].extend(records)

    def get_data(self, s3_path: str, errors_s3_path: str) -> Iterable[pa.Table]:
        """
        Consumes raw PyArrow tables, cleans them via Pydantic,
        and yields cleaned PyArrow tables.
        """
        data_model = ModelMapper.get_data_model(s3_path)
        reader = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET, as_table=True)
        fail_writer = S3FileWriter(s3_path=errors_s3_path, bucket_name=DATA_BUCKET)
        self.schema = VaultSchemaGenerator(model=data_model.schema).run()

        with reader as stream_in, fail_writer as fail_stream_out:
            table: pa.Table
            for table in stream_in:
                raw_dicts = table.to_pylist()
                batch_buffer = {i: defaultdict(list) for i in self.TABLE_TYPES_INSERT_ORDER}
                for raw_record in raw_dicts:
                    try:
                        record: BaseRecord = data_model(**raw_record)
                        vault_payload: VaultedRecordPayload = RecordToVaultConverter(record=record).run()
                        self._add_to_buffer(batch_buffer=batch_buffer, vault_payload=vault_payload)
                    except Exception as e:
                        # Log specific record failures without crashing the whole stream
                        logger.exception(f"Record {raw_record} failed cleaning validation: {e}")
                        fail_stream_out.write_row(raw_record)
                        raise e
                for table_type in self.TABLE_TYPES_INSERT_ORDER:
                    for table_name, records in batch_buffer[table_type].items():
                        if not records:
                            continue
                        # Fetch the correct schema we generated earlier
                        target_schema = self.schema[table_name]
                        print(table_name, records)
                        # Create the Arrow table with strict schema enforcement
                        table = pa.Table.from_pylist(records, schema=target_schema)
                        yield table


if __name__ == "__main__":
    job = S3ToVault()
    data = job.get_data(
        s3_path="silver/business_entity_registry/business_entity_registry.parquet",
        errors_s3_path="errors/loading/business_entity_registry/business_entity_registry.parquet",
    )
    for i in data:
        print(i)
        print("££££££££££££££££££££££££££££££")
