from __future__ import annotations
import os
from typing import Any, Optional, ClassVar

from pydantic import (
    Field,
    model_serializer,
    model_validator,
    field_validator,
    AliasPath,
)
from data_integration_pipeline.io.logger import logger


from data_integration_pipeline.core.data_processing.data_models.templates.base_model_company_name import (
    BaseModelCompanyName,
)
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord

from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema


class SchemaRecord(BaseSchema):
    vendor_id: str
    entity_id: Optional[str]
    company_name: str = Field(validation_alias=AliasPath("company_name", "company_name"))
    company_name_normalized: Optional[str] = Field(default=None, validation_alias=AliasPath("company_name", "company_name_normalized"))
    certification_type: Optional[str]
    trade_specialty: Optional[str]


class ModelCompanyName(BaseModelCompanyName):
    company_name: str = Field(alias="Firm Name", description="Entity name")


class Record(BaseRecord):
    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _data_source: ClassVar[str] = "sub_contractors_registry"
    _upsert_key: ClassVar[str] = "vendor_id"
    _partition_key: ClassVar[str] = "trade_specialty"

    vendor_id: str = Field(alias="Vendor ID", description="Vendor ID")
    entity_id: Optional[str] = Field(alias="Vendor UEI", description="Entity UEI")
    company_name: ModelCompanyName
    certification_type: Optional[str] = Field(default=None, alias="Certification Type", description="Certification type")
    # you could try to convert to NAICS using something like mappings (if available) embeddings
    trade_specialty: Optional[str] = Field(default=None, alias="Trade Specialty", description="Trade specialty")

    @field_validator("entity_id", "vendor_id")
    @classmethod
    def format_id(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.upper().strip()
        return value

    @model_validator(mode="before")
    @classmethod
    def distribute_flat_data(cls, data: Any) -> Any:
        """
        Takes the flat input dictionary and assigns the exact same dictionary
        to every field. The sub-models will filter what they need
        because they have extra='ignore'.
        """
        if not isinstance(data, dict):
            raise Exception(f"Bad data type: {data}")
        data = {k: v if v != "" else None for k, v in data.items()}
        data["company_name"] = ModelCompanyName(**data)
        return data

    @model_serializer(mode="plain")
    def serialize_model(self):
        return {
            "data_source": self._data_source,
            "vendor_id": self.vendor_id,
            "entity_id": self.entity_id,
            **self.company_name.model_dump(),
            "certification_type": self.certification_type,
            "trade_specialty": self.trade_specialty,
        }


if __name__ == "__main__":
    from data_integration_pipeline.settings import TESTS_DATA
    from csv import DictReader

    file_path = os.path.join(TESTS_DATA, "sub_contractors_registry.csv")
    with open(file_path) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            try:
                data_model = Record(**row)
                print(data_model.model_dump())
            except Exception as e:
                logger.error(e)
