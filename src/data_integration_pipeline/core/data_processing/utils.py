from typing import Annotated, Any, Optional

from data_integration_pipeline.io.logger import logger
from pydantic import (
    AfterValidator,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WrapValidator,
)

from data_integration_pipeline.core.data_processing.constants import (
    ONLY_DIGIT_AND_PUNCTUATION_PATTERN,
)


def empty_non_valid(value: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> str:
    try:
        return handler(value)
    except (ValidationError, ValueError) as e:
        logger.warning(f"Emptying {info.field_name}:{value} since it has a validation error: {e}")
        return handler(None)


def is_valid_string(value: Any, info: ValidationInfo):
    if not value:
        return value
    if isinstance(value, str):
        if ONLY_DIGIT_AND_PUNCTUATION_PATTERN.fullmatch(value):
            raise ValueError(f"The field '{info.field_name}' cannot contain only digits or punctuation")
    return value


SoftStr = Annotated[Optional[str], AfterValidator(is_valid_string), WrapValidator(empty_non_valid)]
