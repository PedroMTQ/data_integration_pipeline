from pydantic import (
    BaseModel,
    Field,
    model_serializer,
    model_validator,
)
from typing import Optional

from data_integration_pipeline.core.data_processing.features_extraction.company_name import (
    normalize_company_name,
)
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT


class BaseModelCompanyName(BaseModel):
    model_config = BASE_CONFIG_DICT
    company_name: str = Field(description="Company name")
    company_name_normalized: Optional[str] = Field(default=None, description="Company name normalized for matching")

    @model_validator(mode="after")
    def set_company_name_normalized(self) -> "BaseModelCompanyName":
        if self.company_name_normalized is None and self.company_name:
            self.company_name_normalized = normalize_company_name(self.company_name)
        return self

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict:
        return {
            "company_name": self.company_name,
            "company_name_normalized": self.company_name_normalized,
        }


if __name__ == "__main__":
    data_model = BaseModelCompanyName(company_name="discord inc.")
    print(data_model)
    print(data_model.model_dump())
