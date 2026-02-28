from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.io.logger import logger
from typing import Iterable
from data_integration_pipeline.core.entity_resolution.splink_client import SplinkClient
from splink import SettingsCreator
import splink.comparison_library as cl


# TODO this needs to be dynamic, but for now let's leave it like this
# TODO this is also just a POC, it doesn't work that well
SETTINGS = SettingsCreator(
    # 'link_and_dedupe' handles links between sources AND duplicates within them
    link_type="link_and_dedupe",
    unique_id_column_name="unique_id",
    source_dataset_column_name="source_dataset",
    blocking_rules_to_generate_predictions=[
        "l.entity_id = r.entity_id AND l.entity_id IS NOT NULL",  # Force ID matches
        "soundex(l.company_name_normalized) = soundex(r.company_name_normalized)",
        "l.city = r.city AND l.city IS NOT NULL",
    ],
    comparisons=[
        cl.ExactMatch("entity_id").configure(term_frequency_adjustments=True),
        cl.JaroWinklerAtThresholds("city", [0.9, 0.8]),
        cl.LevenshteinAtThresholds("address_1", [1, 3]),
        cl.JaroWinklerAtThresholds("company_name_normalized", [0.9, 0.8, 0.7]),
    ],
    retain_intermediate_calculation_columns=True,
)


class EntityResolutionJob:
    """
    Processes data from bronze to silver, processes one file at a time to allow for parallelism of tasks
    """

    # we could add a locking mechanism (to avoid racing conditions during parallel work) like in here  https://github.com/PedroMTQ/helical_pdqueiros/blob/main/src/helical_pdqueiros but this is simpler in a POC
    def __init__(self):
        self.s3_client = S3Client()

    def process_data(self, table_names: list[str]) -> str:
        client = SplinkClient(table_names=table_names, settings=SETTINGS)
        links_s3_path = client.run()
        logger.info(f"Finished job and wrote links to {links_s3_path}")
        return links_s3_path

    # TODO you wouldn't do this in prod, you can't just blindly link all data, this POC, we go ahead with it
    def get_data_to_process(self) -> Iterable[dict]:
        res = []
        for table_path in self.s3_client.get_delta_tables(prefix="silver"):
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
