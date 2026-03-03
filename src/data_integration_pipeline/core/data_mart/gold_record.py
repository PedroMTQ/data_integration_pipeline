from __future__ import annotations
from typing import Optional, ClassVar

from pydantic import Field, field_validator
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT
from data_integration_pipeline.core.data_processing.data_models.templates.model_license import ModelLicense


class SchemaRecord(BaseSchema):
    anchor_entity_id: str = Field(description="The primary Unique Entity ID (UEI). Acts as the global anchor for the record.")
    anchor_data_source: str = Field(description="The primary Unique Entity ID (UEI) data source")
    source_entity_id: Optional[str] = Field(default=None, description="The ID as it exists in the original registry")
    source_data_source: Optional[str] = Field(default=None, description="The source of the ID as it exists in the original registry")
    vendor_id: Optional[str] = Field(default=None, description="The internal vendor ID assigned by the Sub-contractors Registry.")
    company_name: str = Field(description="The best-available resolved name for the business after survivorship logic.")
    address_1: Optional[str] = Field(default=None, description="Primary street address of the business headquarters.")
    postal_code: Optional[str] = Field(default=None, description="The 5-digit ZIP or postal code.")
    city: Optional[str] = Field(default=None, description="The city where the business is registered or primarily operates.")
    is_active: Optional[bool] = Field(default=None, description="Indicates if the business is currently marked as active in the source registries.")
    naics_code: Optional[str] = Field(default=None, description="North American Industry Classification System code.")
    naics_code_label: Optional[str] = Field(default=None, description="The human-readable description of the NAICS industry code.")
    certification_type: Optional[str] = Field(default=None, description="Diversity or status certification (e.g., MBE, WBE, DBE, VOSB).")
    trade_specialty: Optional[str] = Field(default=None, description="The primary trade or area of expertise (e.g., Masonry, HVAC, Security).")
    licenses: Optional[list[ModelLicense]] = Field(
        default=None, description="A collection of all associated licenses found across all source records in this cluster."
    )


class Record(BaseRecord):
    """
    The canonical 'Golden Record' for a business entity,
    resolved across Business, License, and Sub-contractor registries.
    """

    model_config = BASE_CONFIG_DICT
    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _data_source: ClassVar[str] = "gold_business_entity"
    _primary_key: ClassVar[str] = ["anchor_entity_id", "anchor_data_source", "source_entity_id", "source_data_source"]
    _partition_key: ClassVar[str] = "city"

    anchor_entity_id: str = Field(
        description="The primary Unique Entity ID (UEI). Acts as the global anchor for the record.", alias="anchor_entity_id"
    )
    anchor_data_source: str = Field(description="The primary Unique Entity ID (UEI) data source", alias="anchor_entity_id")
    source_entity_id: Optional[str] = Field(default=None, description="The ID as it exists in the original registry", alias="alt_entity_id")
    source_data_source: Optional[str] = Field(
        default=None, description="The source of the ID as it exists in the original registry", alias="alt_data_source"
    )
    vendor_id: Optional[str] = Field(default=None, description="The internal vendor ID assigned by the Sub-contractors Registry.")
    company_name: str = Field(description="The best-available resolved name for the business after survivorship logic.")
    address_1: Optional[str] = Field(default=None, description="Primary street address of the business headquarters.")
    postal_code: Optional[str] = Field(default=None, description="The 5-digit ZIP or postal code.")
    city: Optional[str] = Field(default=None, description="The city where the business is registered or primarily operates.")
    is_active: Optional[bool] = Field(default=None, description="Indicates if the business is currently marked as active in the source registries.")
    naics_code: Optional[str] = Field(default=None, description="North American Industry Classification System code.")
    naics_code_label: Optional[str] = Field(default=None, description="The human-readable description of the NAICS industry code.")
    certification_type: Optional[str] = Field(default=None, description="Diversity or status certification (e.g., MBE, WBE, DBE, VOSB).")
    trade_specialty: Optional[str] = Field(default=None, description="The primary trade or area of expertise (e.g., Masonry, HVAC, Security).")
    licenses: Optional[list[ModelLicense]] = Field(
        default=None, description="A collection of all associated licenses found across all source records in this cluster."
    )

    @field_validator("source_entity_id", "source_data_source")
    @classmethod
    def format_id(cls, value: str | None) -> str | None:
        if not value:
            return None
        return value


if __name__ == "__main__":
    record_dict = {
        "entity_id": "UEI1093",
        "anchor_entity_id": "UEI1093",
        "anchor_data_source": "business_entity_registry",
        "anchor_score": 0.8444444444444444,
        "vendor_id": None,
        "splink_id": "business_entity_registry-__-UEI1093",
        "has_id_conflict": True,
        "company_name": "PIEDMONT ELECTRICAL GROUP",
        "address_1": "1782 MAIN STREET",
        "postal_code": "30301",
        "city": "ATLANTA",
        "is_active": False,
        "naics_code": None,
        "naics_code_label": None,
        "certification_type": None,
        "trade_specialty": None,
        "licenses": None,
        "global_score": 0.7143,
    }
    record = Record(**record_dict)
    print(record)
