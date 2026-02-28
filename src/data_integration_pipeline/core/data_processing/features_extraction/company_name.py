from cleanco import basename
from data_integration_pipeline.core.data_processing.constants import REPLACEMENT_TABLE


# we usually go more in depth with this, especially if dealing with multiple geolocations
def normalize_company_name(value: str) -> str:
    if not value:
        return
    value = basename(value)
    return value.translate(REPLACEMENT_TABLE).upper().strip()


if __name__ == "__main__":
    print(normalize_company_name("discord Inc."))
