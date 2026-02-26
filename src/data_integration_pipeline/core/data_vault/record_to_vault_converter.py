from collections import defaultdict
from typing import Any, TypedDict
from pydantic import BaseModel

from data_integration_pipeline.core.data_vault.templates_data_vault import Hub, Link, Satellite
from data_integration_pipeline.core.data_vault.utils import (
    get_hash,
    get_hash_for_payload,
    get_hk,
    get_ldts,
    std,
)
from data_integration_pipeline.settings import (
    FOREIGN_KEY_COLUMN,
    HASH_DIFF_COLUMN,
    LDTS_COLUMN,
    NATURAL_KEY_COLUMN,
    PAYLOAD_KEY,
    PRIMARY_KEY_COLUMN,
    SOURCE_KEY_COLUMN,
)


class VaultedRecordPayload(TypedDict):
    hubs: list[dict[str, Any]]
    links: list[dict[str, Any]]
    satellites: list[dict[str, Any]]


class RecordToVaultConverter:
    def __init__(self, record: BaseModel):
        self.record = record
        self.validated_record = record.schema.model_validate(record)
        self.data_source = record.__class__.data_source
        self.ldts = get_ldts()
        # This will hold the PK of the entity_id field
        self.primary_record_pk = None
        self.hub_pk_registry = {}
        self.satellites_data = {}
        self.hubs_data = defaultdict(list)
        self.links_data = defaultdict(list)
        self.link_satellites_data = {}

    def get_hub_payload(self, src_nk: str) -> dict[str, Any]:
        """
        Generates the Hub PK using the anchor logic (ID + Source Name).
        """
        src_pk = get_hk(source_id=src_nk, source_name=self.data_source)
        return {
            NATURAL_KEY_COLUMN: src_nk,
            SOURCE_KEY_COLUMN: self.data_source,
            PRIMARY_KEY_COLUMN: src_pk,
            LDTS_COLUMN: self.ldts,
        }

    def get_record_satellite_payload(
        self, src_pk: str, payload: dict, list_include_in_hash_diff: list[str]
    ) -> dict[str, Any]:
        """
        Generates the Hub PK using the anchor logic (ID + Source Name).
        """
        diff_payload = {k: v for k, v in payload.items() if k in list_include_in_hash_diff}
        src_hashdiff = get_hash_for_payload(payload=diff_payload)
        return {
            PRIMARY_KEY_COLUMN: src_pk,
            HASH_DIFF_COLUMN: src_hashdiff,
            LDTS_COLUMN: self.ldts,
            SOURCE_KEY_COLUMN: self.data_source,
            PAYLOAD_KEY: payload,
        }

    def get_link_satellite_payload(self, link_pk: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """
        Generates a Satellite specifically for a Link relationship
        using the metadata provided in the Link tag.
        """
        # For Link Satellites, the Hashdiff is often just a hash of the metadata
        # because these relationships are usually static per load.
        hash_diff = get_hash_for_payload(payload=metadata)
        return {
            PRIMARY_KEY_COLUMN: link_pk,  # This is the Link's src_pk
            HASH_DIFF_COLUMN: hash_diff,
            PAYLOAD_KEY: metadata,  # e.g., {'relationship': 'parent'}
            LDTS_COLUMN: self.ldts,
            SOURCE_KEY_COLUMN: self.data_source,
        }

    def get_link_payload(
        self, local_nk: str, remote_nk: str, link_meta: Link, data_source: str
    ) -> dict[str, Any]:
        """
        Generates the Link PK and FKs, plus an optional Link Satellite if metadata is present in the Link definition.
        """
        # 1. Standardize inputs for Link PK generation
        l_nk_std = std(local_nk)
        r_nk_std = std(remote_nk)
        src_std = std(data_source)

        # 2. Generate the Link Primary Key (The Relationship Identity)
        # Consistent directional order: Local -> Remote -> Source
        link_anchor = "||".join([l_nk_std, r_nk_std, src_std])
        link_pk = get_hash(link_anchor)

        # 3. Construct the Core Link Payload
        # FK column names follow the DV2.0 convention.
        # For self-referencing links, we use role-based suffixes (e.g., _child/_parent)
        # to differentiate the two FKs pointing to the same Hub.
        if link_meta.local_hub_name == link_meta.remote_hub_name:
            local_fk_col = f"{FOREIGN_KEY_COLUMN}_{link_meta.local_hub_name}_{link_meta.local_role}"
            remote_fk_col = f"{FOREIGN_KEY_COLUMN}_{link_meta.remote_hub_name}_{link_meta.remote_role}"
        else:
            local_fk_col = f"{FOREIGN_KEY_COLUMN}_{link_meta.local_hub_name}"
            remote_fk_col = f"{FOREIGN_KEY_COLUMN}_{link_meta.remote_hub_name}"

        # FKs must match Hub HKs exactly — pass raw values, get_hk handles std() internally
        link_payload = {
            PRIMARY_KEY_COLUMN: link_pk,
            local_fk_col: get_hk(source_id=local_nk, source_name=data_source),
            remote_fk_col: get_hk(source_id=remote_nk, source_name=data_source),
            SOURCE_KEY_COLUMN: data_source,
            LDTS_COLUMN: self.ldts,
        }

        # 4. Handle Link Satellite (from Metadata)
        # If the Link tag contains metadata, we prepare a satellite record
        link_sat_payload = None
        if link_meta.metadata:
            # HashDiff for the relationship metadata to detect if the logic changes
            link_hash_diff = get_hash_for_payload(payload=link_meta.metadata)
            link_sat_payload = {
                PRIMARY_KEY_COLUMN: link_pk,  # Inherits the Link's PK
                HASH_DIFF_COLUMN: link_hash_diff,
                "src_payload": link_meta.metadata,  # e.g., {'relationship': 'parent'}
                LDTS_COLUMN: self.ldts,
                SOURCE_KEY_COLUMN: data_source,
            }

        return {"link": link_payload, "satellite": link_sat_payload}

    def extract_from_satellite(self, src_pk: str, satellite: Satellite, field_name: str, field_value: Any):
        satellite_name = satellite.name
        if satellite_name not in self.satellites_data:
            self.satellites_data[satellite_name] = {
                "src_pk": src_pk,
                "payload": {},
                "list_include_in_hash_diff": [],
            }
        if satellite.include_in_hash_diff:
            self.satellites_data[satellite_name]["list_include_in_hash_diff"].append(field_name)
        self.satellites_data[satellite_name]["payload"][field_name] = field_value

    def get_satellites_payload(self) -> dict:
        satellites_data = {}
        for satellite_name, satellite_data in self.satellites_data.items():
            satellites_data[satellite_name] = self.get_record_satellite_payload(**satellite_data)
        return satellites_data

    def extract_from_link(self, link: Link, field_name: str, field_value: Any):
        link_name = link.name
        # The local NK is the primary record's natural key (e.g., entity_id)
        local_nk = self.primary_record_nk
        # The remote NK is the value of the field tagged with this Link (e.g., parent ID)
        remote_nk = field_value

        # Build the core link row + optional link-satellite
        link_result = self.get_link_payload(
            local_nk=local_nk,
            remote_nk=remote_nk,
            link_meta=link,
            data_source=self.data_source,
        )

        # Store the core link record
        self.links_data[link_name].append(link_result["link"])

        # Store the link satellite (if metadata was present on the Link tag)
        link_sat = link_result.get("satellite")
        if link_sat:
            sat_link_name = f"sat_{link_name}"
            self.link_satellites_data[sat_link_name] = self.get_link_satellite_payload(
                link_pk=link_result["link"][PRIMARY_KEY_COLUMN],
                metadata=link.metadata,
            )

    def extract_from_hub(self, hub: Hub, field_value: str):
        hub_name = hub.name
        # Generate standard Hub payload
        hub_payload = self.get_hub_payload(src_nk=field_value)
        src_pk = hub_payload[PRIMARY_KEY_COLUMN]
        src_nk = hub_payload[NATURAL_KEY_COLUMN]
        # Only append if this NK hasn't been seen yet (hub deduplication)
        if src_nk not in self.hub_pk_registry:
            self.hubs_data[hub_name].append(hub_payload)
        self.hub_pk_registry[src_nk] = src_pk
        # --- DYNAMIC ANCHOR IDENTIFICATION ---
        if hub.is_primary:
            self.primary_record_pk = src_pk
            # We also store the primary natural key for Link PK generation later
            self.primary_record_nk = src_nk

    def get_links_payload(self) -> dict:
        return dict(self.links_data)

    def run(self) -> VaultedRecordPayload:
        fields = [
            (field_name, field_info, getattr(self.validated_record, field_name))
            for field_name, field_info in self.validated_record.__class__.model_fields.items()
        ]

        # Pass 1: Process all Hubs first to establish anchors
        for _, field_info, field_value in fields:
            if not field_value:
                continue
            for m in field_info.metadata:
                if isinstance(m, Hub):
                    self.extract_from_hub(hub=m, field_value=field_value)

        # Pass 2: Process Satellites and Links (anchors are guaranteed to exist)
        for field_name, field_info, field_value in fields:
            if not field_value:
                continue
            for m in field_info.metadata:
                if isinstance(m, Satellite):
                    self.extract_from_satellite(
                        src_pk=self.primary_record_pk,
                        satellite=m,
                        field_name=field_name,
                        field_value=field_value,
                    )
                elif isinstance(m, Link):
                    self.extract_from_link(link=m, field_name=field_name, field_value=field_value)

        satellites_payload = self.get_satellites_payload()
        satellites_payload.update(self.link_satellites_data)
        links_payload = self.get_links_payload()
        return VaultedRecordPayload(hubs=self.hubs_data, satellites=satellites_payload, links=links_payload)
