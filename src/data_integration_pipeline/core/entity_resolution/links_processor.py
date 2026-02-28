import duckdb
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.delta_client import DeltaClient
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.io.file_reader import S3FileReader
from pathlib import Path
import os
from data_integration_pipeline.settings import SILVER_DATA_FOLDER, DELTA_TABLE_SUFFIX, HASH_DIFF_COLUMN
import pyarrow as pa
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
import pyarrow.compute as pc
import itertools


class LinksProcessor:
    """
    Processes links and extracts respective data for each delta table, yielding all the data so that we can generate integrated records
    """

    COMPOSITE_KEY_SEP = "-__-"

    def __init__(self, metadata: SplinkRunMetadata, bucket_name: str, db_path: str):
        self.metadata = metadata
        self.bucket_name = bucket_name
        self.delta_client = DeltaClient()
        self.db_path = db_path
        if os.path.exists(self.db_path):
            logger.info("Dropping old DB...")
            os.remove(self.db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _wrap_as_record_batch_reader(generator):
        try:
            # 1. Peek at the first item to establish schema
            first_table = next(generator)

            def add_composite_key(table):
                composite_id = pc.binary_join_element_wise(
                    pc.cast(table.column("primary_key_type"), pa.string()),
                    pc.cast(table.column("primary_key_id"), pa.string()),
                    pa.scalar(LinksProcessor.COMPOSITE_KEY_SEP),
                )
                # Append as a brand new column
                return table.append_column("composite_id", composite_id)

            first_table = add_composite_key(first_table)
            schema = first_table.schema

            def batch_stream():
                yield from first_table.to_batches()
                for table in generator:
                    table = add_composite_key(table)
                    # print('other_table', table['composite_id'])
                    yield from table.to_batches()

            return pa.RecordBatchReader.from_batches(schema, batch_stream())
        except StopIteration:
            return None

    def get_data(self):
        """
        joins both the links and the delta tables by cluster so that we can then feed each cluster into the integrated record model and build it from the survivorship rules set there
        """
        with duckdb.connect(self.db_path) as connection:
            source_queries = []
            for table_name in self.metadata.inputs["table_names"]:
                table_path = os.path.join(SILVER_DATA_FOLDER, table_name, f"{table_name}{DELTA_TABLE_SUFFIX}")
                data_model = ModelMapper.get_data_model(table_path)
                primary_key_type = data_model._primary_key
                arrow_table = pa.Table.from_batches(self.delta_client.read(table_path=table_path))
                composite_id_col = pc.binary_join_element_wise(
                    pa.scalar(primary_key_type),
                    pc.cast(arrow_table.column(primary_key_type), pa.string()),
                    pa.scalar(LinksProcessor.COMPOSITE_KEY_SEP),
                )
                standardized_table = arrow_table.append_column("composite_id", composite_id_col)
                standardized_table = standardized_table.drop(HASH_DIFF_COLUMN)
                connection.register(f"raw_{table_name}", standardized_table)
                source_queries.append(f"SELECT * FROM raw_{table_name}")
            union_sql = " UNION ALL BY NAME ".join(source_queries)
            with S3FileReader(s3_path=self.metadata.links_s3_path, bucket_name=self.bucket_name, as_table=True) as reader:
                links_reader = self._wrap_as_record_batch_reader(reader)
                connection.register("links_stream", links_reader)
                if links_reader is None:
                    raise Exception("Links data is missing...")
                connection.register("links_stream", links_reader)
                connection.execute(f"CREATE VIEW all_sources AS {union_sql}")
                # we generally would not get orphans, unless splink for some reason doesn't output unmatched data, we keep with coalesce for safety
                query = """
                SELECT
                    COALESCE(l.cluster_id, 'orphan_' || s.composite_id) as cluster_id,
                    s.*
                FROM all_sources s
                LEFT JOIN (SELECT DISTINCT cluster_id, composite_id FROM links_stream) l
                    ON s.composite_id = l.composite_id
                ORDER BY cluster_id
                """
                return connection.execute(query).fetch_record_batch()

    def run(self):
        data = self.get_data()
        record_stream = (record for batch in data for record in batch.to_pylist())
        # Important: This only works because your SQL has 'ORDER BY cluster_id'
        for _, group in itertools.groupby(record_stream, key=lambda x: x["cluster_id"]):
            # 'group' is an iterator; we convert it to a list for your survivorship logic
            yield list(group)
