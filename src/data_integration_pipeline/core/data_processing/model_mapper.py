from data_integration_pipeline.core.data_processing.data_models.data_sources import (
    LicensesRegistryRecord,
    BusinessEntityRegistryRecord,
    SubContractorsRegistryRecord,
    BaseRecordType,
)
import re
from pathlib import Path
from typing import Type


class ModelMapper:
    MAPPINGS = {
        re.compile(r"business_entity_registry.*", re.IGNORECASE): BusinessEntityRegistryRecord,
        re.compile(r"licenses.*", re.IGNORECASE): LicensesRegistryRecord,
        re.compile(r"sub_contractor.*", re.IGNORECASE): SubContractorsRegistryRecord,
    }

    @staticmethod
    def get_data_model(file_path: str) -> Type[BaseRecordType]:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        Pydantic model class.
        """
        # Extract just the filename (e.g., 'licenses_2026.csv')
        file_name = Path(file_path).name

        for pattern, model_class in ModelMapper.MAPPINGS.items():
            if pattern.match(file_name):
                return model_class
        # Log a warning or return None if no match is found
        return None


if __name__ == "__main__":
    data_model = ModelMapper.get_data_model("business_entity_registry.csv")
    print(data_model)
    data_model = ModelMapper.get_data_model(
        "/home/pedroq/personal/data_integration_pipeline/tmp/bronze/sub_contractors_registry/sub_contractors_registry.csv"
    )
    print(data_model)
