from typing import Any

import great_expectations as gx
from data_integration_pipeline.io.logger import logger
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ModelExpectation(BaseModel):
    column: str = Field(
        description="Mapped column name", default=""
    )  # Default to allow templates expectation_class: Any = Field(description='Expectation class')
    expectation_class: Any = Field(description="Expectation class")
    expectation_kwargs: dict = Field(description="Expectation kwargs")
    expectation: None = Field(default=None, description="Initialized expectation")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def init_gx(self) -> "ModelExpectation":
        try:
            self.expectation = self.expectation_class(column=self.column, **self.expectation_kwargs)
        except Exception as e:
            logger.error(f"Failed to initialize {self.expectation_class.__name__} for {self.column}: {e}")
            raise e
        return self


class ModelExpectationTemplate(BaseModel):
    """The 'Definition' object: Just the rules, no column info yet."""

    expectation_class: Any
    expectation_kwargs: dict = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def apply_to(self, column: str) -> ModelExpectation:
        """The factory method that produces a real, validated ModelExpectation."""
        return ModelExpectation(
            column=column,
            expectation_class=self.expectation_class,
            expectation_kwargs=self.expectation_kwargs,
        )


if __name__ == "__main__":
    expectation = ModelExpectation(
        column="iso2_country_code",
        expectation_class=gx.expectations.ExpectColumnValueLengthsToEqual,
        expectation_kwargs={
            "value": 2,
            "severity": "critical",
            "meta": {
                "description": "Ensures the country code is exactly 2 characters",
                "notes": "If this fails, the downstream data will map to an unknown location",
            },
        },
    )
    print(expectation)
