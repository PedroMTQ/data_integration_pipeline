from __future__ import annotations
from typing import Any, Optional, ClassVar
from collections import Counter

from pydantic import Field, model_serializer, model_validator, BaseModel, computed_field
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.settings import (
    CLUSTER_ID_STR,
    COMPOSITE_ID_STR,
    LDTS_COLUMN,
    HASH_DIFF_COLUMN,
    DATA_SOURCE_STR,
)
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BaseRecord
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT
from data_integration_pipeline.core.data_processing.data_models.templates.model_date import ModelDate



class ModelLicense(BaseModel):
    model_config = BASE_CONFIG_DICT
    _data_source: ClassVar[str] = "licenses_registry"
    license_id: str
    expiration_date: Optional[ModelDate]


