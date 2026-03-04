from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    LicensesRegistryRecord,
    BusinessEntityRegistryRecord,
    SubContractorsRegistryRecord,
    IntegratedRecord,
    GoldRecord,
    BaseRecordType,
)
import re
from typing import Type


class ModelMapper:
    MAPPINGS = {
        re.compile(r'business_entity_registry', re.IGNORECASE): BusinessEntityRegistryRecord,
        re.compile(r'licenses_registry', re.IGNORECASE): LicensesRegistryRecord,
        re.compile(r'sub_contractors_registry', re.IGNORECASE): SubContractorsRegistryRecord,
        re.compile(r'((deduplicated|integrated)_records).*', re.IGNORECASE): IntegratedRecord,
        re.compile(r'(gold_records).*', re.IGNORECASE): GoldRecord,
    }

    @staticmethod
    def get_data_model(file_path: str) -> Type[BaseRecordType] | None:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        Pydantic model class.
        """
        for pattern, model_class in ModelMapper.MAPPINGS.items():
            if pattern.search(file_path):
                return model_class
        # no match
        return None


if __name__ == '__main__':
    data_model = ModelMapper.get_data_model('business_entity_registry.csv')
    print(data_model)
    data_model = ModelMapper.get_data_model(
        '/home/pedroq/personal/data_integration_pipeline/tmp/bronze/sub_contractors_registry/sub_contractors_registry.csv'
    )
    print(data_model)
