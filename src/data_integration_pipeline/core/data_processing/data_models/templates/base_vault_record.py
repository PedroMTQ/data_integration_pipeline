from pydantic import BaseModel, ConfigDict

class BaseVaultRecord(BaseModel):
    """
    this enforces new models to have a data source
    """

    model_config = ConfigDict(strict=True, populate_by_name=True, from_attributes=True)

