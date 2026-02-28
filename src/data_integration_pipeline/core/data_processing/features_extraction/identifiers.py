from data_integration_pipeline.core.data_processing.constants import REPLACEMENT_TABLE


def normalize_id(value: str | None) -> str | None:
    if not value:
        return
    value = str(value)
    return value.translate(REPLACEMENT_TABLE).upper().strip()
