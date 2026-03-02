from __future__ import annotations
import os
from typing import Any, Optional, ClassVar

from pydantic import (
    Field,
    field_validator,
    model_serializer,
    model_validator,
    AliasPath,
)
from data_integration_pipeline.io.logger import logger


from data_integration_pipeline.core.data_processing.data_models.templates.base_model_company_name import (
    BaseModelCompanyName,
)
from data_integration_pipeline.core.data_processing.data_models.templates.base_model_location import (
    BaseModelLocation,
)
from data_integration_pipeline.settings import MIN_LENGTH_ADDRESS_1
from data_integration_pipeline.core.data_processing.data_models.templates.model_date import ModelDate
from data_integration_pipeline.core.data_processing.utils import SoftStr
from data_integration_pipeline.core.data_processing.mappings import NaicsMapping
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema
from data_integration_pipeline.core.data_processing.features_extraction.identifiers import normalize_id


class SchemaRecord(BaseSchema):
    license_id: str
    company_name: str = Field(validation_alias=AliasPath("company_name", "company_name"))
    company_name_normalized: Optional[str] = Field(default=None, validation_alias=AliasPath("company_name", "company_name_normalized"))
    address_1: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    naics_code: Optional[str]
    naics_code_label: Optional[str]
    expiration_date: Optional[ModelDate]


class ModelLocation(BaseModelLocation):
    location: SoftStr = Field(
        default=None,
        alias="HQ LOCATION",
        description="Location",
        min_length=MIN_LENGTH_ADDRESS_1,
        exclude=True,
    )


class ModelCompanyName(BaseModelCompanyName):
    company_name: str = Field(alias="COMPANY NAME", description="Entity name")


class Record(BaseRecord):
    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _data_source: ClassVar[str] = "licenses_registry"
    _primary_key: ClassVar[str] = "license_id"
    _partition_key: ClassVar[str] = "city"

    license_id: str = Field(alias="LICENSE_NUM", description="License number for the company")
    company_name: ModelCompanyName
    location: ModelLocation
    naics_code: Optional[str] = Field(default=None, alias="NAICS CODE", description="NAIC classification code")
    naics_code_label: Optional[str] = Field(default=None, description="NAIC classification code label")
    expiration_date: Optional[ModelDate] = Field(default=None, alias="CERT_EXPIRY_DATE", description="Expiration date of the license")

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
        data["location"] = ModelLocation(**data)
        return data

    @field_validator("license_id")
    @classmethod
    def format_id(cls, value: str | None) -> str | None:
        return normalize_id(value)

    @field_validator("naics_code")
    @classmethod
    def validate_naics_code(cls, value: str | None) -> str | None:
        if not NaicsMapping().get_label(value):
            logger.debug(f"NAICS code {value} not valid, dropping value...")
            return None
        return value

    @model_validator(mode="after")
    def set_naics_label(self) -> "Record":
        # If is_active wasn't provided in the input, calculate it
        if self.naics_code_label is None and self.naics_code:
            self.naics_code_label = NaicsMapping().get_label(self.naics_code)
        return self

    @model_serializer(mode="plain")
    def serialize_model(self):
        return {
            "data_source": self._data_source,
            "license_id": self.license_id,
            **self.company_name.model_dump(),
            "naics_code": self.naics_code,
            "naics_code_label": self.naics_code_label,
            **self.location.model_dump(),
            "expiration_date": self.expiration_date.model_dump() if self.expiration_date else None,
        }


if __name__ == "__main__":
    from data_integration_pipeline.settings import TESTS_DATA
    from csv import DictReader

    file_path = os.path.join(TESTS_DATA, "licenses_registry.csv")
    with open(file_path) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            try:
                data_model = Record(**row)
                print(data_model.model_dump())
            except Exception as e:
                logger.error(e)
