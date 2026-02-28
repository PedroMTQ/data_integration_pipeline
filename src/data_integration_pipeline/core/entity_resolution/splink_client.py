from typing import Iterable
from data_integration_pipeline.io.delta_client import DeltaClient
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
    SPLINK_CLUSTERING_THRESHOLD,
    SPLINK_INFERENCE_PREDICT_THRESHOLD,
)
import os
from data_integration_pipeline.io.logger import logger
import duckdb
from splink import Linker, DuckDBAPI, SettingsCreator
import uuid6


class SplinkClient:
    def __init__(self, table_names: list[str], settings: SettingsCreator):
        self.table_names = table_names
        self.settings = settings
        self.s3_client = S3Client()
        self.delta_client = DeltaClient()
        self.db_path = os.path.join(ER_TEMP, "splink", "er.duckdb")
        if os.path.exists(self.db_path):
            logger.info("Dropping old DB...")
            os.remove(self.db_path)
        self.records_count = {}
        self.data_models_primary_keys = {}

        self.run_id = str(uuid6.uuid7())
        logger.info(f"Starting splink run: {self.run_id}")

    def write_table(self, table_path: str, primary_key: str, table_name: str, schema: pa.Schema) -> str:
        logger.info(f"Reading {table_path}")
        duckdb_client = DuckdbClient(db_path=self.db_path, table_name=table_name)
        data = self.delta_client.read(table_path=table_path)
        data = self.set_to_master_schema(data=data, primary_key=primary_key, table_name=table_name, schema=schema)
        total_rows = duckdb_client.save_to_disk(data)
        self.records_count[table_name] = total_rows
        logger.info(f"Wrote {table_path} to {self.db_path}/{table_name}")

    @staticmethod
    def set_to_master_schema(data: Iterable[pa.Table], primary_key: str, table_name: str, schema: pa.Schema):
        for batch in data:
            batch = batch.append_column("unique_id", batch.column(primary_key).cast(pa.string()))
            batch = batch.append_column("source_dataset", pa.array([table_name] * batch.num_rows))
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
        required_extras = {"unique_id": pa.string(), "source_dataset": pa.string()}
        all_fields.update(required_extras)
        return pa.schema([(name, dtype) for name, dtype in all_fields.items()])

    def write_tables(self) -> list[str]:
        res = []
        schemas = []
        for table_path in self.s3_client.get_delta_tables(prefix="silver"):
            data_model = ModelMapper.get_data_model(table_path)
            schemas.append(data_model._pa_schema)
        master_schema = self.get_master_schema(schemas)
        for table_path in self.s3_client.get_delta_tables(prefix="silver"):
            data_model = ModelMapper.get_data_model(table_path)
            table_name = data_model._data_source
            self.data_models_primary_keys[table_name] = data_model._primary_key
            self.write_table(table_path=table_path, table_name=table_name, primary_key=data_model._primary_key, schema=master_schema)
            res.append(table_name)
        return res

    @staticmethod
    def load_extensions(db_connection):
        db_connection.execute("INSTALL splink_udfs FROM community;")
        db_connection.execute("LOAD splink_udfs;")

    # TODO this always needs to be more customizable
    def get_linker(self, db_api, table_names: list[str], model_path: str, weights_html_path: str):
        if os.path.exists(model_path):
            logger.info(f"Loading pre-trained model from {model_path}")
            return Linker(table_names, model_path, db_api=db_api)

        linker = Linker(table_names, self.settings, db_api=db_api)
        linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
        # 2. EM Session 1: Block on Name
        # This trains the 'entity_id' parameters.
        # Because many records with the same name will have DIFFERENT IDs,
        # the model now sees enough 'non-matches' to train the Else level.
        linker.training.estimate_parameters_using_expectation_maximisation("l.company_name_normalized = r.company_name_normalized")

        # 3. EM Session 2: Block on Postal Code (or another column)
        # This trains the 'company_name_normalized' parameters.
        # It allows the model to see name variations (typos) for the same location.
        linker.training.estimate_parameters_using_expectation_maximisation("l.city = r.city")

        # 4. EM Session 3: Block on entity_id
        # This helps refine weights for other columns (like City/Address)
        # while matching on a very strong identifier.
        linker.training.estimate_parameters_using_expectation_maximisation("l.entity_id = r.entity_id")
        linker.misc.save_model_to_json(model_path, overwrite=True)
        linker.visualisations.match_weights_chart().save(weights_html_path)
        logger.info(f"Trained model saved to {model_path}")
        return linker

    def yield_clustered_records(self, df_clusters, db_connection) -> Iterable[dict]:
        sql = f"""
            SELECT
                cluster_id,
                source_dataset,
                unique_id
            FROM {df_clusters.physical_name}
            ORDER BY cluster_id
            """
        raw_linkage = db_connection.execute(sql).arrow()
        for pa_table in raw_linkage:
            pa_table = pa_table.to_pylist()
            for row in pa_table:
                primary_key_type = self.data_models_primary_keys[row["source_dataset"]]
                row = {
                    CLUSTER_ID_STR: row["cluster_id"],
                    "primary_key_id": row["unique_id"],
                    "primary_key_type": primary_key_type,
                    "data_source": row["source_dataset"],
                }
                print(row)
                yield row

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
        ).to_dict()

    def write_links(self, links_data: Iterable[dict]) -> str:
        s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id, LINKS_FILE_NAME)
        with S3FileWriter(s3_path=s3_path, bucket_name=DATA_BUCKET) as writer:
            for row in links_data:
                writer.write_row(row)
        logger.info(f"Wrote links to {s3_path}")
        return s3_path

    def write_metadata(self, run_metadata: dict) -> str:
        s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id, LINKS_METADATA_FILE_NAME)
        with S3FileWriter(s3_path=s3_path, bucket_name=DATA_BUCKET) as writer:
            writer.write_json(run_metadata)
        logger.info(f"Wrote metadata to {s3_path}")
        return s3_path

    def write_model(self, linker: Linker) -> str:
        s3_path = os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id, LINKS_MODEL_FILE_NAME)
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
        model_path = os.path.join(ER_TEMP, "splink", "er_model.json")
        weights_html_path = os.path.join(ER_TEMP, "splink", "model_weights.html")
        linker = self.get_linker(db_api=db_api, table_names=table_names, model_path=model_path, weights_html_path=weights_html_path)
        logger.info("Starting Splink prediction...")

        predictions = linker.inference.predict(threshold_match_probability=SPLINK_INFERENCE_PREDICT_THRESHOLD)
        df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
            predictions, threshold_match_probability=SPLINK_CLUSTERING_THRESHOLD
        )
        links_data = self.yield_clustered_records(df_clusters=df_clusters, db_connection=db_connection)
        links_s3_path = self.write_links(links_data=links_data)
        run_metadata = self.generate_run_metadata(
            links_s3_path=links_s3_path,
            table_names=table_names,
            linker=linker,
            links_count=self.get_links_count(df_clusters=df_clusters, db_connection=db_connection),
            clusters_count=self.get_clusters_count(df_clusters=df_clusters, db_connection=db_connection),
            records_count=self.records_count,
        )
        self.write_metadata(run_metadata=run_metadata)
        self.write_model(linker=linker)
        return links_s3_path


if __name__ == "__main__":
    client = SplinkClient()
    client.run()
