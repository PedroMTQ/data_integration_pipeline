from __future__ import annotations

from dataclasses import asdict

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    model_serializer,
)

from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT
from data_integration_pipeline.settings import MIN_LENGTH_ADDRESS_1
from data_integration_pipeline.core.data_processing.utils import SoftStr

from data_integration_pipeline.core.data_processing.features_extraction.location import (
    LocationParser,
)


ADDRESS_PARSER = LocationParser()


class BaseModelLocation(BaseModel):
    model_config = BASE_CONFIG_DICT
    location: SoftStr = Field(
        default=None, description="Location", min_length=MIN_LENGTH_ADDRESS_1, exclude=True
    )

    @computed_field(description="Parsed address")
    @property
    def parsed_address(self) -> dict:
        if self.location:
            return asdict(ADDRESS_PARSER.parse(self.location))
        return {}

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict:
        return self.parsed_address


if __name__ == "__main__":
    test_address = "124 POWER AVE, SUITE B, alaska"
    data_model = BaseModelLocation(location=test_address)
    print(data_model)
    print(data_model.model_dump())
