from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.settings import SPLINK_CLUSTERING_THRESHOLD, SPLINK_INFERENCE_PREDICT_THRESHOLD
from typing import Iterable
from data_integration_pipeline.core.entity_resolution.splink_client import SplinkClient
from splink import SettingsCreator
import splink.comparison_library as cl
from splink import block_on

# TODO this needs to be dynamic, but for now let's leave it like this
# TODO this is also just a POC, it doesn't work that well


BLOCKING_RULES = [
    block_on("entity_id"),
    block_on("company_name_normalized"),
    block_on("substr(company_name_normalized, 1, 4)", "city"),
    block_on("substr(company_name_normalized, 1, 4)", "substr(address_1, 1, 5)"),
    block_on("substr(company_name_normalized, 1, 6)"),
]

COMPARISONS = [
    # 1. Entity ID (UEI)
    # An exact match is extremely strong evidence. We use term frequency adjustments
    # just in case there are "placeholder" UEIs used commonly across bad records.
    cl.ExactMatch("entity_id").configure(term_frequency_adjustments=True),
    cl.JaroWinklerAtThresholds("company_name_normalized").configure(term_frequency_adjustments=True),
    cl.JaroWinklerAtThresholds("address_1").configure(term_frequency_adjustments=True),
    cl.JaroWinklerAtThresholds("city"),
]
DETERMINISTIC_RULES = [
    block_on("entity_id"),
    block_on("company_name_normalized", "city"),
    block_on("company_name_normalized", "substr(address_1, 1, 5)"),
]

SETTINGS = SettingsCreator(
    # 'link_and_dedupe' handles links between sources AND duplicates within them
    link_type="link_and_dedupe",
    unique_id_column_name="unique_id",
    source_dataset_column_name="data_source",
    blocking_rules_to_generate_predictions=BLOCKING_RULES,
    comparisons=COMPARISONS,
    retain_intermediate_calculation_columns=True,
    retain_matching_columns=True,
)


class EntityResolutionJob:
    """
    Processes data from bronze to silver, processes one file at a time to allow for parallelism of tasks
    """

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, table_names: list[str]) -> str:
        client = SplinkClient(
            table_names=table_names,
            settings=SETTINGS,
            clustering_threshold=SPLINK_CLUSTERING_THRESHOLD,
            inference_threshold=SPLINK_INFERENCE_PREDICT_THRESHOLD,
            deterministic_rules=DETERMINISTIC_RULES,
        )
        links_s3_path = client.run()
        logger.info(f"Finished job and wrote links to {links_s3_path}")
        return links_s3_path

    def get_data_to_process(self) -> Iterable[dict]:
        res = []
        for table_path in self.s3_client.get_files(prefix="silver", file_name_pattern=r"deduplicated\.parquet"):
            res.append(table_path)
        return res

    def run(self) -> str:
        """
        generic wrapper to run all tasks
        """
        table_names = self.get_data_to_process()
        return self.process_data(table_names)


def process_task(table_names: list[dict]):
    job = EntityResolutionJob()
    silver_s3_path = job.process_data(table_names)
    return silver_s3_path


def get_tasks() -> list[dict]:
    job = EntityResolutionJob()
    return job.get_data_to_process()


if __name__ == "__main__":
    job = EntityResolutionJob()
    job.run()
