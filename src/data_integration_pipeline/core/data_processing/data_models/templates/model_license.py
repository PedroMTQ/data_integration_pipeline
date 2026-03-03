from __future__ import annotations
from typing import Optional, ClassVar

from pydantic import BaseModel
from data_integration_pipeline.core.data_processing.data_models.templates.base_record import BASE_CONFIG_DICT
from data_integration_pipeline.core.data_processing.data_models.templates.model_date import ModelDate


class ModelLicense(BaseModel):
    model_config = BASE_CONFIG_DICT
    _data_source: ClassVar[str] = "licenses_registry"
    license_id: str
    expiration_date: Optional[ModelDate]
