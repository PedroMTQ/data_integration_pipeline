import great_expectations as gx
from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    LicensesRegistryRecord,
    BusinessEntityRegistryRecord,
    SubContractorsRegistryRecord,
)

from data_integration_pipeline.core.audits.expectation_data_model import ModelExpectationTemplate
from data_integration_pipeline.core.audits.data_auditor import DataAuditor
from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import SILVER_DATA_FOLDER, DATA_BUCKET
from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
from data_integration_pipeline.core.audits.s3_weighted_data_sampler import S3WeightedParquetSampler


class AuditSilverDataJob:
    def __init__(self):
        self.s3_client = S3Client(bucket_name=DATA_BUCKET)

    def run(self) -> bool:
        additional_rules = (
            {
                "patterns": ["entity_id", "vendor_id", "license_id"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToBeUnique,
                        expectation_kwargs={
                            "severity": "critical",
                            "meta": {
                                "description": "Ensures IDs dont repeat",
                                "notes": "If this fails, there is a data duplication issue.",
                            },
                        },
                    )
                ],
            },
        )
        weight_column_mapping = {
            BusinessEntityRegistryRecord: "city",
            LicensesRegistryRecord: "naics_code",
            # this would need to be properly split or standerdized so that we could have a distributed by trade
            SubContractorsRegistryRecord: "trade_specialty",
        }
        for silver_s3_path in self.s3_client.get_delta_tables(prefix=SILVER_DATA_FOLDER):
            data_model = ModelMapper.get_data_model(silver_s3_path)
            s3_sampler = S3WeightedParquetSampler(
                s3_path=silver_s3_path,
                weight_column=weight_column_mapping[data_model],
                target_total_rows=1000,
            )
            data_auditor = DataAuditor(data_model=data_model, dataset_stage="silver", additional_rules=additional_rules)
            data_sample = s3_sampler.get_data()
            data_auditor.run(data=data_sample)
            data_auditor.export_docs()


if __name__ == "__main__":
    job = AuditSilverDataJob()
    job.run()
