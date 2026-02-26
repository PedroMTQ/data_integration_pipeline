import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from data_integration_pipeline.settings import (
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    PRIMARY_KEY_COLUMN,
)


def std(v: Any) -> str:
    """
    Standardizes values for hashing: Trim, Upper, and Null handling.
    """
    # 1. Handle actual Python None
    if v is None:
        return "^~"

    # 2. Stringify and clean
    clean_v = str(v).strip()

    # 3. Handle Empty Strings (treating them as Nulls)
    if clean_v == "":
        return "^~"

    # 4. Return Uppercased version
    return clean_v.upper()


def get_hash(str_to_encode: str) -> str:
    """
    Standardizes and hashes a single business key.
    """
    if str_to_encode is None:
        # Data Vault standard: Use a placeholder for NULLs
        # (e.g., -1 or a specific MD5 of an empty string)
        str_to_encode = "~~NULL~~"
    # 2. Hash (MD5 is common for performance; SHA-256 for security)
    return hashlib.sha256(std(str_to_encode).encode("utf-8")).hexdigest()


def get_hk(source_id: str, source_name: str) -> str:
    """
    Generates a Data Vault 2.0 compliant SHA-256 Hash Key (HK).
    This is the 'Anchor' for your Hub.
    """
    if not source_id or not source_name:
        raise Exception("Missing source_id or source_name")
    s_id = std(source_id)
    s_name = std(source_name)
    # Standard DV2.0: Separator prevents '1'+'23' colliding with '12'+'3'
    anchor_key_id = f"{s_id}||{s_name}"
    return get_hash(anchor_key_id)


def get_hash_for_payload(payload: dict[str, Any]) -> str:
    """
    Generates a SHA-256 Hash Diff for change detection in Satellites or for other dicts (e.g., links pks).
    Excludes the business key (anchor) and existing metadata.
    """
    return get_hash(prepare_payload_for_hashing(payload))


def prepare_payload_for_hashing(payload: dict[str, Any]) -> str:
    # Use your sorted logic to ensure determinism
    # We exclude the anchor key because changes to IDs are new entities, not updates.
    std_payload = [std(v) for k, v in sorted(payload.items())]
    return "||".join(std_payload)


def get_ldts() -> datetime:
    return datetime.now(timezone.utc)


def prepare_for_arrow(record: dict[str, Any]) -> dict[str, Any]:
    """
    Scans a dictionary and converts any nested dicts or lists
    into JSON strings to satisfy the 'dict: pa.string()' requirement.
    """
    for key, value in record.items():
        # If it's a list (like messaging or phone_numbers)
        if isinstance(value, list):
            # We check if the items inside are dicts/objects that need stringifying
            # (Phone numbers are already strings, so they remain strings)
            record[key] = [json.dumps(item) if isinstance(item, (dict, list)) else item for item in value]
        # If it's a standalone dictionary (like incorporation_date)
        elif isinstance(value, dict):
            record[key] = json.dumps(value)

    return record


def update_schema_for_data_vault(schema: pa.Schema):
    vault_fields = [
        pa.field(PRIMARY_KEY_COLUMN, pa.string(), nullable=False),
        pa.field(HASH_DIFF_COLUMN, pa.string(), nullable=False),
        pa.field(LDTS_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False),
    ]
    for field in vault_fields:
        schema: pa.Schema = schema.append(field)
    return schema
