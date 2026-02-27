from pydantic import BaseModel, ConfigDict
from typing import ClassVar
from data_integration_pipeline.core.data_processing.data_models.templates.base_schema import BaseSchema

from data_integration_pipeline.core.schema_converter import PyarrowSchemaGenerator
import pyarrow as pa


BASE_CONFIG_DICT = ConfigDict(populate_by_name=True, extra="ignore", str_strip_whitespace=True, arbitrary_types_allowed=True)


class BaseRecord(BaseModel):
    """
    Base model for data source records
    """

    model_config = BASE_CONFIG_DICT
    _record_schema: ClassVar[BaseSchema]
    _data_source: ClassVar[str]
    _upsert_key: ClassVar[str]
    _partition_key: ClassVar[str] = None
    _pa_schema: ClassVar[pa.Schema]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for attr in ["_record_schema", "_data_source", "_upsert_key"]:
            # Check if the subclass has defined data_source and it's not the base version
            if not hasattr(cls, attr):
                raise TypeError(f"Class {cls.__name__} must define a unique <{attr}> ClassVar.")
        cls._pa_schema = PyarrowSchemaGenerator(cls._record_schema).run()
