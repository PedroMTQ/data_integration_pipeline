from __future__ import annotations

from typing import Annotated, Any, ClassVar, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_serializer,
    model_validator,
)

from data_integration_pipeline.core.data_vault.templates_data_vault import Hub, Link, Satellite
from data_integration_pipeline.core.data_processing.data_models.templates.base_vault_record import BaseVaultRecord


class GoldenCompanyPyarrowSchema(BaseVaultRecord):
    """
    Unified schema for Golden Records.
    """

    # --- IDENTIFIERS (HUB & LINKS) ---

    # Primary Golden Hub (from business entity registry)
    entity_id: Annotated[str, Hub(record_source="entity_regitry", name="entities", is_primary=True)] = Field()
    # Link to Vendor Hub (from subcontractors registry)
    vendor_id: Annotated[
        Optional[str],
        Hub(record_source="subcontractors_registry", name="vendors"),
        Link(
            name="company_to_vendor",
            local_hub_name="companies",
            remote_hub_name="vendors",
            metadata={"source": "sea_procurement"},
        ),
        Satellite(name="identifiers", hub_name="companies"),
    ] = Field(default=None)

    # Link to License Hub (from licenses registry)
    license_id: Annotated[
        Optional[str],
        Hub(record_source="licenses_registry", name="licenses"),
        Link(
            name="company_to_license",
            local_hub_name="companies",
            remote_hub_name="licenses",
            metadata={"type": "trade_license"},
        ),
        Satellite(name="identifiers", hub_name="companies"),
    ] = Field(default=None)

    # --- NAMES (SATELLITE) ---
    company_name: Annotated[str, Satellite(name="names", hub_name="entities")] = Field()
    company_name_normalized: Annotated[Optional[str], Satellite(name="names", hub_name="entities")] = Field(
        default=None
    )

    # --- LOCATION (SATELLITE) ---
    address_1: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(default=None)
    postal_code: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(
        default=None
    )
    city: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(default=None)

    # --- DESCRIPTIONS & INDUSTRY (SATELLITE) ---
    certification_type: Annotated[Optional[str], Satellite(name="descriptions", hub_name="entities")] = Field(
        default=None
    )
    trade_specialty: Annotated[Optional[str], Satellite(name="descriptions", hub_name="entities")] = Field(
        default=None
    )
    naics_code: Annotated[Optional[str], Satellite(name="descriptions", hub_name="entities")] = Field(
        default=None
    )

    # --- STATUS ---
    is_active: Annotated[bool, Satellite(name="status", hub_name="entities")] = Field(default=True)


class GoldenCompanyRecord(BaseModel):
    """
    Class to represent a Golden Record for a company.
    Receives a dictionary with the raw data from the different sources and distributes it to the different satellites.
    Manages survivorship rules and data quality.
    """

    SURVIVORSHIP_RULES: ClassVar[dict] = {
        "default": ["entity_data", "vendor_data", "license_data"],
        "features": {
            "entity_id": ["entity_data", "vendor_data"],
            "address_1": ["entity_data", "license_data"],
            "address_2": ["entity_data", "license_data"],
            "postal_code": ["entity_data", "license_data"],
            "city": ["entity_data", "license_data"],
            "state": ["entity_data", "license_data"],
        },
    }
    model_config = ConfigDict(extra="ignore")
    schema: ClassVar[BaseModel] = GoldenCompanyPyarrowSchema

    # We define the fields flat to match the expected output of the validator
    entity_id: str = Field(description="Entity ID")
    vendor_id: Optional[str] = Field(default=None, description="Vendor ID")
    license_id: Optional[str] = Field(default=None, description="License ID")
    company_name: str = Field(description="Company Name")
    company_name_normalized: Optional[str] = Field(default=None, description="Normalized Company Name")
    address_1: Optional[str] = Field(default=None, description="Address 1")
    postal_code: Optional[str] = Field(default=None, description="Postal Code")
    city: Optional[str] = Field(default=None, description="City")
    certification_type: Optional[str] = Field(default=None, description="Certification Type")
    trade_specialty: Optional[str] = Field(default=None, description="Trade Specialty")
    naics_code: Optional[str] = Field(default=None, description="NAICS Code")
    naics_code_label: Optional[str] = Field(default=None, description="NAICS Code Label")

    @model_validator(mode="before")
    @classmethod
    def apply_survivorship(cls, data: Any) -> Any:
        # 1. Validates data sources and typess
        for data_source, values in data.items():
            if data_source not in cls.SURVIVORSHIP_RULES["default"]:
                raise Exception(f"Invalid data source: {data_source}")
            if not isinstance(values, dict):
                raise Exception(f"Invalid data source data type: {data_source} - {type(values)}")
        # 2. Define the target fields based on the class annotations
        target_fields = [f for f in cls.model_fields.keys() if f != "schema"]
        res = {}
        # 3. Apply Survivorship per field
        for field in target_fields:
            # Determine the source priority for this field
            ranking = cls.SURVIVORSHIP_RULES["features"].get(field, cls.SURVIVORSHIP_RULES["default"])

            # The first source in the ranking that has a value wins
            for data_source in ranking:
                source_value = data.get(data_source, {}).get(field, None)
                if source_value is not None:
                    res[field] = source_value
                    break
        return res

    @model_serializer(mode="plain")
    def serialize_model(self):
        """
        Validates the flat data against the Pyarrow schema (which handles
        the AliasPath nesting for Hubs/Sats) and returns the final dict.
        """
        return self.schema.model_validate(self).model_dump()


if __name__ == "__main__":
    raw_data = {
        "vendor_data": {
            "vendor_id": "SEA-4432",
            "entity_id": "UEI_9821",
            "company_name": "North West Piping",
            "company_name_normalized": "North West Piping",
            "certification_type": "DBE",
            "trade_specialty": "Plumbing, Utilities",
        },
        "entity_data": {
            "entity_id": "UEI_9821",
            "company_name": "ELITE ELECTRICAL SYSTEMS LLC",
            "company_name_normalized": "ELITE ELECTRICAL SYSTEMS",
            "address_1": "123 POWER AVE",
            "postal_code": "10001",
            "city": "10001",
            "is_active": True,
        },
        "license_data": {
            "license_id": "ST-221",
            "company_name": "ELITE ELEC SYS LLC",
            "company_name_normalized": "ELITE ELEC SYS",
            "naics_code": "238120",
            "naics_code_label": "Structural Steel and Precast Concrete Contractors",
            "address_1": "88 INDUSTRIAL PARKWAY",
            "address_2": None,
            "postal_code": None,
            "city": None,
            "state": None,
        },
    }

    golden_record = GoldenCompanyRecord(**raw_data).model_dump()
    print(golden_record)
