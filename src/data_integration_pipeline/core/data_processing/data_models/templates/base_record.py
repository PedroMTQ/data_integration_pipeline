from pydantic import BaseModel, ConfigDict
from typing import ClassVar
from data_integration_pipeline.core.data_processing.data_models.templates.base_vault_record import BaseVaultRecord

BASE_CONFIG_DICT = ConfigDict(
    populate_by_name=True, extra="ignore", str_strip_whitespace=True, arbitrary_types_allowed=True
)


class BaseRecord(BaseModel):
    """
    this enforces new models to have a data source
    """

    model_config = BASE_CONFIG_DICT
    data_source: ClassVar[str]
    schema: ClassVar[BaseVaultRecord]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Check if the subclass has defined data_source and it's not the base version
        if not hasattr(cls, "data_source"):
            raise TypeError(f"Class {cls.__name__} must define a unique 'data_source' ClassVar.")
        if not hasattr(cls, "schema"):
            raise TypeError(f"Class {cls.__name__} must define a unique 'schema' ClassVar.")
