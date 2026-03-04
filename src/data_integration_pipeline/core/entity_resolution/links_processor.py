import duckdb
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.io.file_reader import S3FileReader
from pathlib import Path
import os
from data_integration_pipeline.settings import (
    DATA_BUCKET,
    SILVER_DATA_FOLDER,
    PARQUET_TABLE_SUFFIX,
    HASH_DIFF_COLUMN,
    COMPOSITE_ID_STR,
    DATA_SOURCE_STR,
    COMPOSITE_KEY_SEP,
)
import pyarrow as pa
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
import pyarrow.compute as pc
import itertools


class LinksProcessor:
    """
    Processes links and extracts respective data for each delta table, yielding all the data so that we can generate integrated records
    """

    def __init__(self, metadata: SplinkRunMetadata, db_path: str, bucket_name: str = DATA_BUCKET):
        self.metadata = metadata
        self.bucket_name = bucket_name
        self.db_path = db_path
        if os.path.exists(self.db_path):
            logger.info('Dropping old DB...')
            os.remove(self.db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _prepare_links(table):
        composite_id = pc.binary_join_element_wise(
            pc.cast(table.column('primary_key_type'), pa.string()),
            pc.cast(table.column('primary_key_id'), pa.string()),
            pa.scalar(COMPOSITE_KEY_SEP),
        )
        return table.append_column(COMPOSITE_ID_STR, composite_id)

    @staticmethod
    def _prepare_data(table, primary_key_type: str, table_name: str):
        composite_id_col = pc.binary_join_element_wise(
            pa.scalar(primary_key_type),
            # this is the primary_key_id like in the links table
            pc.cast(table.column(primary_key_type), pa.string()),
            pa.scalar(COMPOSITE_KEY_SEP),
        )
        standardized_table = table.append_column(COMPOSITE_ID_STR, composite_id_col)
        data_source_col = pa.array([table_name] * len(standardized_table), type=pa.string())
        standardized_table = standardized_table.append_column(DATA_SOURCE_STR, data_source_col)
        standardized_table = standardized_table.drop(HASH_DIFF_COLUMN)
        return standardized_table

    @staticmethod
    def _wrap_as_record_batch_reader(generator, functions_to_run: list[dict]):
        try:
            first_table = next(generator)
            for function_to_run in functions_to_run:
                function = function_to_run['function']
                kwargs = function_to_run.get('kwargs', {})
                first_table = function(first_table, **kwargs)
            schema = first_table.schema

            def batch_stream():
                yield from first_table.to_batches()
                for table in generator:
                    for function_to_run in functions_to_run:
                        function = function_to_run['function']
                        kwargs = function_to_run.get('kwargs', {})
                        table = function(table, **kwargs)
                    yield from table.to_batches()

            return pa.RecordBatchReader.from_batches(schema, batch_stream())
        except StopIteration:
            return None

    def get_links(self, db_connection):
        # note that the links table has is a flat table where each row represents a link, and not a cluster, i.e., if you have a cluster with 2 links, then you will have 2 rows in this table pointing to the same cluster
        logger.info(f'Reading links from {self.metadata.links_s3_path}')
        reader = S3FileReader(s3_path=self.metadata.links_s3_path, bucket_name=self.bucket_name, as_table=True)
        links_reader = self._wrap_as_record_batch_reader(reader, functions_to_run=[{'function': self._prepare_links}])
        db_connection.register('links_stream', links_reader)
        if links_reader is None:
            raise Exception('Links data is missing...')
        db_connection.register('links_stream', links_reader)

    def get_data(self, db_connection):
        source_queries = []
        for table_name in self.metadata.inputs['table_names']:
            data_table_name = f'data_{table_name}'
            table_path = os.path.join(SILVER_DATA_FOLDER, table_name, f'deduplicated{PARQUET_TABLE_SUFFIX}')
            logger.info(f'Reading data from {table_path}')
            data_model = ModelMapper.get_data_model(table_path)
            primary_key_type = data_model._primary_key
            reader = S3FileReader(s3_path=table_path, bucket_name=self.bucket_name, as_table=True)
            data_reader = self._wrap_as_record_batch_reader(
                reader,
                functions_to_run=[{'function': self._prepare_data, 'kwargs': {'primary_key_type': primary_key_type, 'table_name': table_name}}],
            )
            db_connection.register(data_table_name, data_reader)
            source_queries.append(f'SELECT * FROM {data_table_name}')
        union_sql = ' UNION ALL BY NAME '.join(source_queries)
        return union_sql

    def get_links_with_data(self):
        """
        joins both the links and the delta tables by cluster so that we can then feed each cluster into the integrated record model and build it from the survivorship rules set there
        """
        with duckdb.connect(self.db_path) as connection:
            union_sql = self.get_data(db_connection=connection)
            self.get_links(db_connection=connection)
            connection.execute('DROP VIEW IF EXISTS all_sources')
            connection.execute(f'CREATE VIEW all_sources AS {union_sql}')
            # we generally would not get orphans, unless splink for some reason doesn't output unmatched data, we keep with coalesce for safety
            query = f"""
            SELECT
                COALESCE(l.cluster_id, 'orphan_' || s.{COMPOSITE_ID_STR}) as cluster_id,
                s.*
            FROM all_sources s
            LEFT JOIN (SELECT DISTINCT cluster_id, {COMPOSITE_ID_STR} FROM links_stream) l
                ON s.{COMPOSITE_ID_STR} = l.{COMPOSITE_ID_STR}
            ORDER BY cluster_id
            """
            return connection.execute(query).fetch_record_batch()

    def run(self):
        data = self.get_links_with_data()
        record_stream = (record for batch in data for record in batch.to_pylist())
        # Important: This only works because your SQL has 'ORDER BY cluster_id'
        for _, group in itertools.groupby(record_stream, key=lambda x: x['cluster_id']):
            # 'group' is an iterator; we convert it to a list for your survivorship logic
            cluster_records = list(group)
            yield cluster_records
