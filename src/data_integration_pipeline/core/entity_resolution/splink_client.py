from typing import Iterable
from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.io.file_writer import S3FileWriter
from data_integration_pipeline.io.duckdb_client import DuckdbClient
import pyarrow as pa
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from data_integration_pipeline.settings import (
    ER_TEMP,
    ENTITY_RESOLUTION_DATA_FOLDER,
    LINKS_FILE_NAME,
    LINKS_METADATA_FILE_NAME,
    CLUSTER_ID_STR,
    LINKS_MODEL_FILE_NAME,
    DATA_BUCKET,
)
import os
from data_integration_pipeline.io.logger import logger
import duckdb
from splink import Linker, DuckDBAPI, SettingsCreator
import uuid6
from collections import defaultdict
from splink import block_on
from data_integration_pipeline.io.file_reader import S3FileReader


class SplinkClient:
    def __init__(self, table_names: list[str], settings: SettingsCreator, clustering_threshold: float, inference_threshold: float, deterministic_rules: list):
        self.table_names = table_names
        self.settings = settings
        self.clustering_threshold = clustering_threshold
        self.inference_threshold = inference_threshold
        self.deterministic_rules = deterministic_rules
        self.s3_client = S3Client()
        self.db_path = os.path.join(ER_TEMP, "splink", "er.duckdb")
        if os.path.exists(self.db_path):
            logger.info("Dropping old DB...")
            os.remove(self.db_path)
        self.records_count = {}
        self.data_models_primary_keys = {}
        # TODO the model should actually be archived in Mlflow, but for now, we just store it locally
        self.model_path = os.path.join(ER_TEMP, "splink", "er_model.json")
        self.run_id = str(uuid6.uuid7())
        self.s3_folder = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id)
        logger.info(f"Starting splink run: {self.run_id}")

    def write_table(self, table_path: str, primary_key: str, table_name: str, schema: pa.Schema) -> str:
        logger.info(f"Reading {table_path}")
        duckdb_client = DuckdbClient(db_path=self.db_path, table_name=table_name)
        with S3FileReader(s3_path=table_path, bucket_name=DATA_BUCKET, as_table=True) as reader:
            data = self.set_to_master_schema(data=reader, primary_key=primary_key, table_name=table_name, schema=schema)
            total_rows = duckdb_client.save_to_disk(data)
            self.records_count[table_name] = total_rows
            logger.info(f"Wrote {table_path} to {self.db_path}/{table_name}")

    @staticmethod
    def set_to_master_schema(data: Iterable[pa.Table], primary_key: str, table_name: str, schema: pa.Schema):
        for batch in data:
            batch = batch.append_column("unique_id", batch.column(primary_key).cast(pa.string()))
            batch = batch.append_column("data_source", pa.array([table_name] * batch.num_rows))
            for field in schema:
                if field.name not in batch.column_names:
                    null_array = pa.nulls(batch.num_rows, type=field.type)
                    batch = batch.append_column(field.name, null_array)
            yield batch.select(schema.names).cast(schema)

    @staticmethod
    def get_master_schema(schemas: list[pa.Schema]) -> pa.Schema:
        all_fields = {}
        for schema in schemas:
            for field in schema:
                if field.name not in all_fields:
                    all_fields[field.name] = field.type
        required_extras = {"unique_id": pa.string(), "data_source": pa.string()}
        all_fields.update(required_extras)
        return pa.schema([(name, dtype) for name, dtype in all_fields.items()])

    def write_tables(self) -> list[str]:
        res = []
        schemas = []
        for table_path in self.table_names:
            data_model = ModelMapper.get_data_model(table_path)
            schemas.append(data_model._pa_schema)
        master_schema = self.get_master_schema(schemas)

        for table_path in self.table_names:
            data_model = ModelMapper.get_data_model(table_path)
            data_source = data_model._data_source
            self.data_models_primary_keys[data_source] = data_model._primary_key
            self.write_table(table_path=table_path, table_name=data_source, primary_key=data_model._primary_key, schema=master_schema)
            res.append(data_source)
        return res

    @staticmethod
    def load_extensions(db_connection):
        db_connection.execute("INSTALL splink_udfs FROM community;")
        db_connection.execute("LOAD splink_udfs;")

    # TODO this always needs to be more customizable
    def get_linker(self, db_api, table_names: list[str]):
        if os.path.exists(self.model_path):
            logger.info(f"Loading pre-trained model from {self.model_path}")
            return Linker(table_names, settings=self.model_path, db_api=db_api)

        linker = Linker(table_names, settings=self.settings, db_api=db_api)

        linker.training.estimate_probability_two_random_records_match(self.deterministic_rules, recall=0.7)
        linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
        # EM Session 1: Block on exact Name
        # This block has plenty of data across ALL THREE datasets.
        # It will successfully train 'entity_id', 'city', and 'address_1'
        linker.training.estimate_parameters_using_expectation_maximisation(block_on("company_name_normalized"))
        # EM Session 2: Block on Entity ID
        # This block spans Business <-> Subs (and within).
        # It trains 'company_name_normalized' by looking at pairs with the same UEI.
        linker.training.estimate_parameters_using_expectation_maximisation(block_on("entity_id"))

        # EM Session 3: Block on fuzzy Name (First 4 chars)
        # This casts a wide net to train variations across all features.
        linker.training.estimate_parameters_using_expectation_maximisation(block_on("substr(company_name_normalized, 1, 4)"))

        linker.misc.save_model_to_json(self.model_path, overwrite=True)
        logger.info(f"Trained model saved to {self.model_path}")
        return linker

    def yield_clustered_records(self, df_clusters, db_connection) -> Iterable[dict]:
        sql = f"""
            SELECT
                cluster_id,
                data_source,
                unique_id
            FROM {df_clusters.physical_name}
            ORDER BY cluster_id
            """
        raw_linkage = db_connection.execute(sql).arrow()
        cluster_map = defaultdict(set)
        overlap_counts = defaultdict(int)
        for pa_table in raw_linkage:
            pa_table = pa_table.to_pylist()
            for row in pa_table:
                primary_key_type = self.data_models_primary_keys[row["data_source"]]
                row = {
                    CLUSTER_ID_STR: row["cluster_id"],
                    "primary_key_id": row["unique_id"],
                    "primary_key_type": primary_key_type,
                    "data_source": row["data_source"],
                }
                cluster_map[row["cluster_id"]].add(row["data_source"])
                yield row
        for sources in cluster_map.values():
            # Create a sorted string signature like "registry_a + registry_b"
            signature = " + ".join(sorted(list(sources)))
            overlap_counts[signature] += 1
        self.overlap_report = dict(overlap_counts)

    def get_clusters_count(self, df_clusters, db_connection) -> int:
        """
        Returns the number of unique Golden Records (entities) created.
        """
        # Count distinct cluster_ids in the clustered table
        sql = f"SELECT COUNT(DISTINCT cluster_id) FROM {df_clusters.physical_name}"
        result = db_connection.execute(sql).fetchone()
        return result[0] if result else 0

    def get_links_count(self, df_clusters, db_connection) -> int:
        count_sql = f"SELECT COUNT(1) FROM {df_clusters.physical_name}"
        return db_connection.execute(count_sql).fetchone()[0]

    def generate_run_metadata(
        self, links_s3_path: str, table_names, linker: Linker, links_count: int, clusters_count: int, records_count: dict[str, int]
    ) -> dict:
        return SplinkRunMetadata.from_splink(
            run_id=self.run_id,
            links_s3_path=links_s3_path,
            table_names=table_names,
            linker=linker,
            links_count=links_count,
            clusters_count=clusters_count,
            records_count=records_count,
            overlap_report=self.overlap_report,
        ).to_dict()

    def write_links(self, links_data: Iterable[dict]) -> str:
        s3_path = os.path.join(self.s3_folder, LINKS_FILE_NAME)
        with S3FileWriter(s3_path=s3_path, bucket_name=DATA_BUCKET) as writer:
            for row in links_data:
                writer.write_row(row)
        logger.info(f"Wrote links to {s3_path}")
        return s3_path

    def write_metadata(self, run_metadata: dict) -> str:
        s3_path = os.path.join(self.s3_folder, LINKS_METADATA_FILE_NAME)
        with S3FileWriter(s3_path=s3_path, bucket_name=DATA_BUCKET) as writer:
            writer.write_json(run_metadata)
        logger.info(f"Wrote metadata to {s3_path}")
        return s3_path

    def write_model(self, linker: Linker) -> str:
        s3_path = os.path.join(self.s3_folder, LINKS_MODEL_FILE_NAME)
        with S3FileWriter(s3_path=s3_path, bucket_name=DATA_BUCKET) as writer:
            writer.write_json(linker._settings_obj.as_dict())
        logger.info(f"Wrote model to {s3_path}")
        return s3_path


    def run(self):
        # we retrain all the time, but in a prod env, we would stick with a pre-trained EM model
        table_names = self.write_tables()
        db_connection = duckdb.connect(self.db_path)
        self.load_extensions(db_connection=db_connection)
        db_api = DuckDBAPI(connection=db_connection)
        linker = self.get_linker(db_api=db_api, table_names=table_names)
        logger.info("Starting Splink prediction...")

        predictions = linker.inference.predict(threshold_match_probability=self.inference_threshold)
        df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(predictions, threshold_match_probability=self.clustering_threshold)
        links_data = self.yield_clustered_records(df_clusters=df_clusters, db_connection=db_connection)
        links_s3_path = self.write_links(links_data=links_data)
        links_count = (self.get_links_count(df_clusters=df_clusters, db_connection=db_connection),)
        clusters_count = self.get_clusters_count(df_clusters=df_clusters, db_connection=db_connection)
        run_metadata = self.generate_run_metadata(
            links_s3_path=links_s3_path,
            table_names=table_names,
            linker=linker,
            links_count=links_count,
            clusters_count=clusters_count,
            records_count=self.records_count,
        )
        logger.info(f"Splink run results: {run_metadata}")
        self.write_metadata(run_metadata=run_metadata)
        self.write_model(linker=linker)
        os.remove(self.db_path)
        return links_s3_path


if __name__ == "__main__":
    client = SplinkClient()
    client.run()
