from __future__ import annotations
from typing import Any, Optional, ClassVar

from pydantic import (
    Field,
    model_serializer,
    model_validator,
    BaseModel,
)
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT
from data_integration_pipeline.core.data_processing.data_models.templates.model_date import ModelDate


class ModelLicense(BaseModel):
    model_config = BASE_CONFIG_DICT
    license_id: str
    expiration_date: Optional[ModelDate]


class SchemaRecord(BaseSchema):
    entity_id: Optional[str] = Field(description="Entity UEI")
    vendor_id: Optional[str] = Field(description="Entity vendor ID")
    company_name: str = Field(description="Entity name")
    address_1: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    is_active: Optional[bool]
    license_ids: Optional[list[str]]
    naics_code: Optional[str]
    naics_code_label: Optional[str]
    licenses: Optional[list[ModelLicense]]
    certification_type: Optional[str]
    trade_specialty: Optional[str]


class Record(BaseRecord):
    """
    Each integrated record is derived from one or more profiles.
    A profile can represent multiple business entities/sub contractors which can in turn have multiple licenses
    We define the integrated entity id by checking what is the entity with the highest level of agreement and depth on a cluster.

    Note that the entities are transitory, and each time we perform new linking, they can change.
    For that we need to do this in 3 steps:
    - create integrated records
    - create a id bridge mapping each id to their parent (or itself if the record is the parent)
    - create a view with the final gold record (for the downstream user)

    """

    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _data_source: ClassVar[str] = "gold_business_entity"
    _primary_key: ClassVar[str] = "entity_id"
    _partition_key: ClassVar[str] = "city"

    integrated_entity_id: str = Field("Entity UEI of the highest fidelity record")
    splink_id: Optional[str] = Field(
        description="Splink ID if sourced from links. This is transient and is newly derived in each Splink run and is used exclusively for auditing the ER process"
    )
    entity_ids: list[str] = Field(description="Entity UEI")
    has_id_conflict: bool = Field(default=False)
    vendor_ids: Optional[list[str]] = Field(description="List of entity vendor ID")
    license_ids: Optional[list[str]]

    company_name: str = Field(description="Entity name")
    address_1: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    is_active: Optional[bool]
    naics_code: Optional[str]
    naics_code_label: Optional[str]
    licenses: Optional[list[ModelLicense]]
    certification_type: Optional[str]
    trade_specialty: Optional[str]

    @model_validator(mode="before")
    @classmethod
    def distribute_cluster_data(cls, data: Any) -> Any:
        """
        receives cluster data from multiple data sources, including multiple records from the same data source
        e.g.:
        [{'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': 'UEI1000', 'company_name': 'LIBERTY HVAC SOLUTIONS LLC', 'company_name_normalized': 'LIBERTY HVAC SOLUTIONS', 'address_1': '3555 COMMERCE WAY', 'postal_code': '33101', 'city': 'MIAMI', 'is_active': True, 'ldts': datetime.datetime(2026, 2, 28, 14, 37, 21, 556764, tzinfo=<DstTzInfo 'Europe/Luxembourg' CET+1:00:00 STD>), 'composite_id': 'entity_id-__-UEI1000', 'license_id': None, 'naics_code': None, 'naics_code_label': None, 'expiration_date': None, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}, {'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': None, 'company_name': 'Liberty Climate Pro', 'company_name_normalized': 'LIBERTY CLIMATE PRO', 'address_1': '3555 COMMERCE WAY', 'postal_code': None, 'city': 'PHOENIX', 'is_active': None, 'ldts': datetime.datetime(2026, 2, 28, 14, 37, 22, 157134, tzinfo=<DstTzInfo 'Europe/Luxembourg' CET+1:00:00 STD>), 'composite_id': 'license_id-__-ST1000', 'license_id': 'ST1000', 'naics_code': None, 'naics_code_label': None, 'expiration_date': {'year': 2026, 'month': 9, 'day': 14}, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}]


        
        """
        raise Exception
        if not isinstance(data, dict):
            raise Exception(f"Bad data type: {data}")
        data = {k: v if v != "" else None for k, v in data.items()}
        data["company_name"] = ModelCompanyName(**data)
        return data

    @model_validator(mode="after")
    def validate_id_consistency(self) -> "Record":
        """
        Critical check: If we have multiple IDs, we must flag it for the business.
        """
        if len(self.alt_entity_ids) > 0:
            self.has_id_conflict = True
            logger.warning(f"Cluster {self.splink_id} has ID conflict: {self.entity_id} vs {self.alt_entity_ids}")
        return self

    @model_serializer(mode="plain")
    def serialize_model(self):
        return {
            "data_source": self._data_source,
            "entity_id": self.entity_id,
            **self.company_name.model_dump(),
            "address_1": self.address_1,
            "postal_code": self.postal_code,
            "city": self.city,
            "is_active": self.is_active,
        }


if __name__ == "__main__":
    from data_integration_pipeline.settings import TESTS_DATA
    from csv import DictReader
    from datetime import datetime
    cluster_data = [
        {'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': None, 'company_name': 'Liberty Climate Pro', 'company_name_normalized': 'LIBERTY CLIMATE PRO', 'address_1': '3555 COMMERCE WAY', 'postal_code': None, 'city': 'PHOENIX', 'is_active': None, 'ldts': datetime.now(), 'composite_id': 'license_id-__-ST1000', 'license_id': 'ST1000', 'naics_code': None, 'naics_code_label': None, 'expiration_date': {'year': 2026, 'month': 9, 'day': 14}, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}, 
        {'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': 'UEI1000', 'company_name': 'LIBERTY HVAC SOLUTIONS LLC', 'company_name_normalized': 'LIBERTY HVAC SOLUTIONS', 'address_1': '3555 COMMERCE WAY', 'postal_code': '33101', 'city': 'MIAMI', 'is_active': True, 'ldts': datetime.now(), 'composite_id': 'entity_id-__-UEI1000', 'license_id': None, 'naics_code': None, 'naics_code_label': None, 'expiration_date': None, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}
        ]
