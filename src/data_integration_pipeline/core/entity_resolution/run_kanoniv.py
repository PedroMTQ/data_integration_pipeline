from kanoniv import Spec, Source, validate, reconcile
import pyarrow.parquet as pq
from typing import Iterable
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.io.s3_client import S3Client
import pyarrow as pa
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.settings import ER_SPEC_PATH


class KanonivClient():
    def __init__(self):
        self.s3_client = S3Client()
        self.delta_client = DeltaClient()

    def get_data_sources(self):
        return self.s3_client.get_delta_tables(prefix='silver')

    def get_sources(self) -> Iterable[Source]:
        for table_name in self.get_data_sources():
            data_model = ModelMapper.get_data_model(table_name)
            table = pa.concat_tables(self.delta_client.read(table_name=table_name))
            yield Source.from_arrow(data_model._data_source, table, primary_key=data_model._upsert_key)

    def run(self):
        sources = list(self.get_sources())
        spec = Spec.from_file(ER_SPEC_PATH)
        print(spec)


if __name__ == '__main__':
    client = KanonivClient()
    client.run()