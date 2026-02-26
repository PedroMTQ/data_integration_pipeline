from __future__ import annotations
import os
from typing import Any, Optional, ClassVar, Annotated

from pydantic import (
    Field,
    computed_field,
    model_serializer,
    model_validator,
    field_validator,
    AliasPath,
)
from data_integration_pipeline.io.logger import logger


from data_integration_pipeline.core.data_processing.data_models.templates.base_model_company_name import (
    BaseModelCompanyName,
)
from data_integration_pipeline.core.data_processing.utils import SoftStr
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_vault.templates_data_vault import Hub, Satellite
from data_integration_pipeline.core.data_processing.data_models.templates.base_vault_record import BaseVaultRecord


class VaultRecord(BaseVaultRecord):
    # natural key for entities hub
    entity_id: Annotated[str, Hub(record_source="entity_regitry", name="entities", is_primary=True)] = Field()

    # --- NAMES (SATELLITE) ---
    company_name: Annotated[str, Satellite(name="names", hub_name="entities")] = Field(validation_alias=AliasPath('company_name', 'company_name'))
    company_name_normalized: Annotated[Optional[str], Satellite(name="names", hub_name="entities")] = Field(default=None,validation_alias=AliasPath('company_name', 'company_name_normalized'))

    # --- LOCATION (SATELLITE) ---
    address_1: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(default=None)
    postal_code: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(default=None)
    city: Annotated[Optional[str], Satellite(name="location", hub_name="entities")] = Field(default=None)

    # --- STATUS (SATELLITE)---
    is_active: Annotated[bool, Satellite(name="status", hub_name="entities")] = Field(default=True)


class ModelCompanyName(BaseModelCompanyName):
    company_name: str = Field(alias="Official Business Name", description="Entity name")


class Record(BaseRecord):
    data_source: ClassVar[str] = "business_entity_registry"
    schema: ClassVar[BaseVaultRecord] = VaultRecord

    entity_id: str = Field(alias="Entity UEI", description="Entity UEI")
    company_name: ModelCompanyName
    address_1: SoftStr = Field(default=None, alias="Address Line 1")
    postal_code: Optional[str] = Field(default=None, alias="Zip Code")
    city: SoftStr = Field(default=None, alias="City Name")
    entity_status: Optional[str] = Field(
        default=None, alias="Registration Status", description="Entity status", exclude=True
    )

    @field_validator("entity_id")
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

    @computed_field(description="Status of company")
    @property
    def is_active(self) -> bool:
        if self.entity_status == "ACT":
            return True
        else:
            return False

    @model_serializer(mode="plain")
    def serialize_model(self):
        return {
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

    file_path = os.path.join(TESTS_DATA, "business_entity_registry.csv")
    with open(file_path) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            try:
                data_model = Record(**row)
                print(data_model.model_dump())
            except Exception as e:
                logger.error(e)
