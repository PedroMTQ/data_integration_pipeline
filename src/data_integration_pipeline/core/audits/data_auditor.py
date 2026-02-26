import fnmatch
import os
from typing import Any, Iterable, Type, Literal

import great_expectations as gx
import pyarrow as pa
from data_integration_pipeline.io.logger import logger
from pydantic import BaseModel
from data_integration_pipeline.core.audits.expectation_data_model import (
    ModelExpectation,
    ModelExpectationTemplate,
)
from data_integration_pipeline.settings import TEMP
from data_integration_pipeline.core.schema_converter import get_pyarrow_schema



class DataAuditor:
    data_source_name: str = "pandas"
    # ! tried with duckdb but kept getting this error: "list index out of range". doesn't seem very well supported

    def __init__(
        self,
        data_model: Type[BaseModel],
        dataset_stage: Literal["bronze", "silver", "gold"],
        additional_rules: list[dict] = None,
        rebuild_suite: bool = True,
    ):
        self.audit_columns = get_pyarrow_schema(data_model.schema).names
        self.additional_rules = additional_rules
        self._rebuild_suite = rebuild_suite

        data_suffix = f"{data_model.data_source}_{dataset_stage}"
        self.batch_definition_name = f"sample_{data_suffix}"
        self.duckdb_table_name = f"audit_{data_suffix}"
        self.run_name = f"audit_{data_suffix}"
        self.data_asset_name = data_model.data_source
        self.suite_name = f"suite_{data_suffix}"
        self.validation_definition_name = f"validation_definition_{data_suffix}"

        self.run_id = gx.RunIdentifier(run_name=self.run_name)
        self.context = gx.get_context(mode="file", project_root_dir=os.path.join(TEMP, "audits"))
        self.db_path = os.path.join(TEMP, 'audits', 'duckdb', 'audit.db')
        self.__setup_expectations()


    def _get_expectations_definitions(self) -> list[dict[str, Any]]:
        res = [
            {
                # I include both the alias mappings (which have a prefix) and serialized model mappings
                "patterns": ["*_id"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToBeUnique,
                        expectation_kwargs={
                            "severity": "warning",
                            "mostly": 0.95,
                            "meta": {
                                "description": "Ensures IDs dont repeat too much",
                                "notes": "If this fails, there is likely a data duplication issue.",
                            },
                        },
                    )
                ],
            },
            {
                "patterns": ["company_name"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                        expectation_kwargs={"mostly": 1.0, "severity": "critical"},
                    ),
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToBeUnique,
                        expectation_kwargs={"mostly": 0.7, "severity": "warning"},
                    ),
                ],
            },
            {
                "patterns": ["company_name_normalized"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                        expectation_kwargs={"mostly": 0.95, "severity": "critical"},
                    )
                ],
            },
            {
                "patterns": ["address_1"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                        expectation_kwargs={"mostly": 0.3, "severity": "warning"},
                    ),
                ],
            },
            {
                "patterns": ["zip_code"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValueLengthsToBeBetween,
                        expectation_kwargs={
                            "min_value": 3,
                            "max_value": 12,
                            "mostly": 0.80,
                            "severity": "warning",
                        },
                    ),
                ],
            },
            {
                "patterns": ["city"],
                "rules": [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                        expectation_kwargs={"mostly": 0.7, "severity": "warning"},
                    ),
                ],
            },
        ]
        if self.additional_rules:
            # We assume additional_rules follows the same {"pattern": str, "rules": [...]} format
            res.extend(self.additional_rules)
        return res

    def __setup_expectations(self):
        definitions = self._get_expectations_definitions()
        self.expectations = []
        for entry in definitions:
            # 1. Uncompress pattern to actual columns
            column_patterns = entry.get("patterns")
            rules = entry.get("rules")
            if not column_patterns:
                raise Exception(f"Invalid rule (missing pattern key): {entry}")
            if not rules:
                raise Exception(f"Invalid rule (missing rules key): {entry}")
            matched_cols = set()
            for p in column_patterns:
                matches = fnmatch.filter(self.audit_columns, p)
                matched_cols.update(matches)

            for col in matched_cols:
                expectation_template: ModelExpectationTemplate
                for expectation_template in rules:
                    if not expectation_template:
                        continue
                    # 2. Bind the template to the specific column (this triggers validation
                    try:
                        expectation_model: ModelExpectation = expectation_template.apply_to(col)
                        self.expectations.append(expectation_model)
                    except Exception as e:
                        logger.exception(f"Failed to add {col} ruke: {expectation_model}")
                        raise e

    def __setup_data_source(self):
        # Add Data Source
        try:
            self.datasource = self.context.data_sources.get(self.data_source_name)
        except KeyError:
            self.datasource = self.context.data_sources.add_pandas(name=self.data_source_name)

    def __setup_data_asset(self):

        # Add Data Asset
        try:
            self.data_asset = self.datasource.get_asset(self.data_asset_name)
        except LookupError:
            self.data_asset = self.datasource.add_dataframe_asset(name=self.data_asset_name)

    def __setup_batch_def(self):

        # Add Batch Definition (Required for GX 1.x)
        try:
            self.batch_definition = self.data_asset.get_batch_definition(self.batch_definition_name)
        except KeyError:
            self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(name=self.batch_definition_name)

    def __setup_suite(self):
        if self._rebuild_suite:
            try:
                self.context.suites.delete(self.suite_name)
            except Exception:
                pass  # Handle if it didn't exist
            self.suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
            self.__build_suite()
        else:
            try:
                # Try to get existing suite
                self.suite = self.context.suites.get(self.suite_name)
                # Logic: If your code's definitions have changed, update the store
                self.__build_suite()
                self.context.suites.add_or_update(self.suite)
            except gx.exceptions.DataContextError:
                # Create new if it doesn't exist
                self.suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
                self.__build_suite()

    def __setup_val_definition(self):
        try:
            self.val_definition = self.context.validation_definitions.get(self.validation_definition_name)
        except gx.exceptions.DataContextError:
            self.val_definition = self.context.validation_definitions.add(gx.ValidationDefinition(name=self.validation_definition_name, data=self.batch_definition, suite=self.suite))

    def __build_suite(self):
        for expectation_model in self.expectations:
            self.suite.add_expectation(expectation_model.expectation)

    def __process_results(self, results: list[dict]) -> bool:
        exception_failures = [r for r in results if not r["success"] and r["exception_info"].get('exception_info',{})]
        critical_failures = [r for r in results if not r["success"] and r["expectation_config"]["meta"].get("severity") == "critical"]
        warning_failures = [r for r in results if not r["success"] and r["expectation_config"]["meta"].get("severity") == "warning"]
        info_failures = [r for r in results if not r["success"] and r["expectation_config"]["meta"].get("severity") == "info"]
        # 4. Define your "Business Logic" for success
        if exception_failures:
            logger.critical(f"❌ AUDIT FAILED: {len(exception_failures)} exceptions found.")
            # Logic to handle hard stop (e.g., return False or raise Exception)
            return False
        if critical_failures:
            logger.critical(f"❌ AUDIT FAILED: {len(critical_failures)} critical errors found.")
            # Logic to handle hard stop (e.g., return False or raise Exception)
            return False
        if warning_failures:
            logger.warning(f"⚠️ AUDIT PASSED WITH WARNINGS: {len(warning_failures)} issues detected.")
            # You can still return True here so the DuckDB dump proceeds
            return True
        if info_failures:
            logger.info(f"⚠️ AUDIT PASSED WITH INFO: {len(info_failures)} issues detected.")
            # You can still return True here so the DuckDB dump proceeds
            return True
        logger.info("✅ AUDIT PASSED: All expectations met.")
        return True

    def export_docs(self):
        self.context.build_data_docs()

    def run(self, data: Iterable[pa.Table]) -> bool:
        # TODO
        df = pa.concat_tables(data).to_pandas()
        self.__setup_data_source()
        self.__setup_suite()
        self.__setup_data_asset()
        self.__setup_batch_def()
        self.__setup_val_definition()
        results = self.val_definition.run(batch_parameters={"dataframe": df}, run_id=self.run_id)
        return self.__process_results(results=results.get("results", {}))


if __name__ == "__main__":
    from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
    from data_integration_pipeline.core.audits.s3_weighted_data_sampler import S3WeightedParquetSampler

    s3_path = "silver/business_entity_registry/business_entity_registry.parquet"
    data_model = ModelMapper.get_data_model(s3_path)
    s3_sampler = S3WeightedParquetSampler(
        s3_path=s3_path,
        bucket_name="data",
        weight_column="city",
        # weights={"NEW YORK": 50.0},
        default_weight=1,
        target_total_rows=100,
    )
    # Consume the generator to trigger the sampling
    print("get_total_raw_records", s3_sampler.get_total_raw_records())
    print("get_raw_data_distribution", s3_sampler.get_raw_data_distribution())
    print("get_total_sampled_records", s3_sampler.get_total_sampled_records())
    print("get_sample_data_distribution", s3_sampler.get_sample_data_distribution())
    data_auditor = DataAuditor(data_model=data_model, dataset_stage="silver")
    data_sample = s3_sampler.get_data()
    results = data_auditor.run(data=data_sample)
    print(results)
    data_auditor.export_docs()
