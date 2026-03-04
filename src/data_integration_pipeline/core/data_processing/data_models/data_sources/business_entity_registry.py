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
from data_integration_pipeline.core.data_processing.features_extraction.location import AddressStandardizer
from data_integration_pipeline.core.data_processing.features_extraction.identifiers import normalize_id
from data_integration_pipeline.core.data_processing.utils import SoftStr
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema
from data_integration_pipeline.core.data_processing.features_extraction.location import (
    LocationParser,
)


class SchemaRecord(BaseSchema):
    entity_id: str
    company_name: str = Field(validation_alias=AliasPath('company_name', 'company_name'))
    company_name_normalized: Optional[str] = Field(default=None, validation_alias=AliasPath('company_name', 'company_name_normalized'))
    address_1: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    is_active: bool


class ModelCompanyName(BaseModelCompanyName):
    company_name: str = Field(alias='Official Business Name', description='Entity name')


class Record(BaseRecord):
    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _data_source: ClassVar[str] = 'business_entity_registry'
    _primary_key: ClassVar[str] = 'entity_id'
    _partition_key: ClassVar[str] = 'city'

    entity_id: str = Field(alias='Entity UEI', description='Entity UEI')
    company_name: ModelCompanyName
    address_1: SoftStr = Field(default=None, alias='Address Line 1')
    postal_code: Optional[str] = Field(default=None, alias='Zip Code')
    city: SoftStr = Field(default=None, alias='City Name')
    entity_status: Optional[str] = Field(default=None, alias='Registration Status', description='Entity status', exclude=True)
    is_active: bool = Field(default=None, description='Entity is active/inactive')

    @field_validator('entity_id')
    @classmethod
    def format_id(cls, value: str | None) -> str | None:
        return normalize_id(value)

    @field_validator('address_1')
    @classmethod
    def clean_address_1(cls, value: str | None) -> str | None:
        address_parser = LocationParser()
        parsed_location = address_parser.parse(value)
        if parsed_location.address_1:
            value = AddressStandardizer().replace_abbreviations(parsed_location.address_1)
        return value

    @field_validator('city', 'address_1', 'postal_code')
    @classmethod
    def format_location(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.upper().strip()
        return value

    @model_validator(mode='before')
    @classmethod
    def distribute_flat_data(cls, data: Any) -> Any:
        """
        Takes the flat input dictionary and assigns the exact same dictionary
        to every field. The sub-models will filter what they need
        because they have extra='ignore'.
        """
        if not isinstance(data, dict):
            raise Exception(f'Bad data type: {data}')
        data = {k: v if v != '' else None for k, v in data.items()}
        data['company_name'] = ModelCompanyName(**data)
        return data

    @model_validator(mode='after')
    def set_active_status(self) -> 'Record':
        # If is_active wasn't provided in the input, calculate it
        if self.is_active is None:
            self.is_active = self.entity_status == 'ACT'
        return self

    @model_serializer(mode='plain')
    def serialize_model(self):
        return {
            'data_source': self._data_source,
            'entity_id': self.entity_id,
            **self.company_name.model_dump(),
            'address_1': self.address_1,
            'postal_code': self.postal_code,
            'city': self.city,
            'is_active': self.is_active,
        }


if __name__ == '__main__':
    from data_integration_pipeline.settings import TESTS_DATA
    from csv import DictReader

    file_path = os.path.join(TESTS_DATA, 'business_entity_registry.csv')
    with open(file_path) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            try:
                data_model = Record(**row)
                print(data_model.model_dump())
                print(data_model._pa_schema)
            except Exception as e:
                logger.error(e)
            break
