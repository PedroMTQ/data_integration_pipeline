from collections import defaultdict
from typing import Any, TypedDict, Union
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
from data_integration_pipeline.io.logger import logger


class VaultedRecordPayload(TypedDict):
    hubs: list[dict[str, Any]]
    links: list[dict[str, Any]]
    satellites: list[dict[str, Any]]


class RecordToVaultConverter:
    def __init__(self, record: BaseModel):
        self.record = record
        self.data_source = record.data_source
        self.ldts = get_ldts()
        # This will hold the PK of the entity_id field
        self.primary_record_pk = None
        self.hub_pk_registry = defaultdict(str)  # Key: Hub Name -> PK
        self.hub_nk_registry = defaultdict(str)  # Key: Hub Name -> PK
        self.link_pk_registry = defaultdict(dict)  # Key: Link Name -> Relationship Key -> PK
        self.resolved_parent_pks = {}  # Key: Table Name -> PK
        self.satellites_data = {}
        self.hubs_data = defaultdict(list)
        self.links_data = defaultdict(list)
        self.link_satellites_data = {}

    def get_table_name(self, table_item: Union[Hub, Satellite, Link]):
        if isinstance(table_item, Hub):
            return {"hub_name": f"hub_{table_item.name}"}
        elif isinstance(table_item, Link):
            return {
                "link_name": f"lnk_{table_item.name}",
                "local_hub_name": f"hub_{table_item.local_hub_name}",
                "remote_hub_name": f"hub_{table_item.remote_hub_name}",
            }
        elif isinstance(table_item, Satellite):
            if table_item.hub_name:
                satellite_name = f"sat_hub_{table_item.hub_name}_{table_item.name}"
                src_pk_table_name = f"hub_{table_item.hub_name}"
            elif table_item.link_name:
                satellite_name = f"sat_lnk_{table_item.link_name}_{table_item.name}"
                src_pk_table_name = f"lnk_{table_item.link_name}"
            return {"satellite_name": satellite_name, "src_pk_table_name": src_pk_table_name}

    def get_hub_payload(self, src_nk: str, data_source: str, id_type: str) -> dict[str, Any]:
        """
        Generates the Hub PK using the anchor logic (ID + Source Name).
        """
        src_pk = get_hk(source_id=src_nk, id_type=id_type)
        return {
            NATURAL_KEY_COLUMN: src_nk,
            SOURCE_KEY_COLUMN: data_source,
            PRIMARY_KEY_COLUMN: src_pk,
            LDTS_COLUMN: self.ldts,
        }

    def get_satellite_payload(
        self,
        src_pk: str,
        payload: dict,
        data_source: str,
        list_exclude_in_hash_diff: list[str] = None,
    ) -> dict[str, Any]:
        """
        Generates the Hub PK using the anchor logic (ID + Source Name).
        """
        if list_exclude_in_hash_diff:
            diff_payload = {k: v for k, v in payload.items() if k not in list_exclude_in_hash_diff}
        else:
            diff_payload = dict(payload)
        src_hashdiff = get_hash_for_payload(payload=diff_payload)
        return {
            PRIMARY_KEY_COLUMN: src_pk,
            HASH_DIFF_COLUMN: src_hashdiff,
            LDTS_COLUMN: self.ldts,
            SOURCE_KEY_COLUMN: data_source,
            PAYLOAD_KEY: payload,
        }

    def get_link_payload(self, local_nk: str, remote_nk: str, link: Link, data_source: str) -> dict[str, Any]:
        """
        Generates the Link PK and FKs, plus an optional Link Satellite if metadata is present in the Link definition.
        """
        # 1. Standardize inputs for Link PK generation
        local_nk_std = std(local_nk)
        remote_nk_std = std(remote_nk)
        sorted_hubs = sorted([(link.local_hub_name, local_nk_std), (link.remote_hub_name, remote_nk_std)], key=lambda x: x[0])

        src_std = std(data_source)
        # 2. Generate the Link Primary Key (The Relationship Identity)
        # Consistent directional order: Local -> Remote -> Source
        link_anchor = "||".join([sorted_hubs[0][1], sorted_hubs[1][1], src_std])
        link_pk = get_hash(link_anchor)

        # 3. Construct the Core Link Payload
        # FK column names follow the DV2.0 convention.
        # For self-referencing links, we use role-based suffixes (e.g., _child/_parent)
        # to differentiate the two FKs pointing to the same Hub.
        if link.local_hub_name == link.remote_hub_name:
            local_fk_col = f"{FOREIGN_KEY_COLUMN}_{link.local_hub_name}_{link.local_role}"
            remote_fk_col = f"{FOREIGN_KEY_COLUMN}_{link.remote_hub_name}_{link.remote_role}"
        else:
            local_fk_col = f"{FOREIGN_KEY_COLUMN}_{link.local_hub_name}"
            remote_fk_col = f"{FOREIGN_KEY_COLUMN}_{link.remote_hub_name}"
        # if the roles are not the default ones, we assume there is some semantic meaning to them and therefore store the info in a satellite
        if link.local_role != "1" and link.remote_role != "2":
            relationship_metadata = {
                link.local_hub_name: link.local_role,
                link.remote_hub_name: link.remote_role,
            }
            if link.metadata is None:
                link.metadata = {}
            link.metadata.update(relationship_metadata)

        # FKs must match Hub HKs exactly — pass raw values, get_hk handles std() internally
        link_payload = {
            PRIMARY_KEY_COLUMN: link_pk,
            local_fk_col: get_hk(source_id=local_nk, id_type=link.local_id_type),
            remote_fk_col: get_hk(source_id=remote_nk, id_type=link.remote_id_type),
            SOURCE_KEY_COLUMN: data_source,
            LDTS_COLUMN: self.ldts,
        }

        # 4. Handle Link Satellite (from Metadata)
        # If the Link tag contains metadata, we prepare a satellite record
        link_sat_payload = None
        if link.metadata:
            # HashDiff for the relationship metadata to detect if the logic changes
            link_hash_diff = get_hash_for_payload(payload=link.metadata)
            link_sat_payload = {
                PRIMARY_KEY_COLUMN: link_pk,  # Inherits the Link's PK
                HASH_DIFF_COLUMN: link_hash_diff,
                PAYLOAD_KEY: link.metadata,  # e.g., {'relationship': 'parent'}
                LDTS_COLUMN: self.ldts,
                SOURCE_KEY_COLUMN: data_source,
            }

        return {"link": link_payload, "satellite": link_sat_payload}

    def extract_from_satellite(self, satellite: Satellite, field_name: str, field_value: Any):
        table_prefixes = self.get_table_name(satellite)
        satellite_name = table_prefixes["satellite_name"]
        src_pk_table_name = table_prefixes["src_pk_table_name"]
        if satellite.hub_name:
            src_pk = self.hub_pk_registry[src_pk_table_name]
        elif satellite.link_name:
            src_pk = self.link_pk_registry[src_pk_table_name]
        if satellite_name not in self.satellites_data:
            self.satellites_data[satellite_name] = {
                "src_pk": src_pk,
                "payload": {},
                "list_exclude_in_hash_diff": [],
                "data_source": self.data_source,
            }
        if not satellite.include_in_hash_diff:
            self.satellites_data[satellite_name]["list_exclude_in_hash_diff"].append(field_name)
        self.satellites_data[satellite_name][PAYLOAD_KEY][field_name] = field_value

    def extract_from_link(self, link: Link, field_value: Any):
        table_prefixes = self.get_table_name(link)

        link_name = table_prefixes["link_name"]
        local_hub_name = table_prefixes["local_hub_name"]
        local_nk = self.hub_nk_registry[local_hub_name]
        remote_nk = field_value
        if not local_nk or not remote_nk:
            logger.error(f"Invalid link: {link} for record {self.record}")
            return
        # Build the core link row + optional link-satellite
        link_result = self.get_link_payload(local_nk=local_nk, remote_nk=remote_nk, link=link, data_source=self.data_source)

        # Store the core link record
        self.links_data[link_name].append(link_result["link"])

        # Store the link satellite (if metadata was present on the Link tag)
        link_sat = link_result.get("satellite")
        if link_sat:
            sat_link_name = f"sat_{link_name}"
            self.link_satellites_data[sat_link_name] = self.get_satellite_payload(
                src_pk=link_result["link"][PRIMARY_KEY_COLUMN],
                payload=link.metadata,
                # some links are not inherent to the data, but to specific linking mechanisms e.g., during entity resolution
                data_source=link.data_source or self.data_source,
            )

    def get_satellites_payload(self) -> dict:
        satellites_data = {}
        for satellite_name, satellite_data in self.satellites_data.items():
            satellites_data[satellite_name] = self.get_satellite_payload(**satellite_data)
        satellites_data.update(self.link_satellites_data)
        return satellites_data

    def extract_from_hub(self, hub: Hub, field_value: str):
        table_prefixes = self.get_table_name(hub)
        hub_name = table_prefixes["hub_name"]
        # Generate standard Hub payload
        hub_payload = self.get_hub_payload(src_nk=field_value, data_source=self.data_source, id_type=hub.id_type)
        src_pk = hub_payload[PRIMARY_KEY_COLUMN]
        src_nk = hub_payload[NATURAL_KEY_COLUMN]
        # Only append if this NK hasn't been seen yet (hub deduplication)
        if src_nk not in self.hub_pk_registry:
            self.hubs_data[hub_name].append(hub_payload)
        self.hub_nk_registry[hub_name] = src_nk
        self.hub_pk_registry[hub_name] = src_pk

    def run(self) -> VaultedRecordPayload:
        validated_record = self.record.schema.model_validate(self.record)
        record_dump = validated_record.model_dump()
        model_fields = validated_record.__class__.model_fields
        fields_to_process = []
        for field_name, field_info in model_fields.items():
            val = record_dump.get(field_name)
            if val is not None:
                fields_to_process.append((field_name, field_info, val))

        # Pass 1: Process all Hubs first to establish anchors
        for _, field_info, field_value in fields_to_process:
            if not field_value:
                continue
            for metadata_instance in field_info.metadata:
                if isinstance(metadata_instance, Hub):
                    self.extract_from_hub(hub=metadata_instance, field_value=field_value)

        # Pass 2: Process Satellites and Links (anchors are guaranteed to exist)
        for field_name, field_info, field_value in fields_to_process:
            if not field_value:
                continue
            for metadata_instance in field_info.metadata:
                if isinstance(metadata_instance, Satellite):
                    self.extract_from_satellite(satellite=metadata_instance, field_name=field_name, field_value=field_value)
                elif isinstance(metadata_instance, Link):
                    self.extract_from_link(link=metadata_instance, field_value=field_value)

        return VaultedRecordPayload(hubs=self.hubs_data, satellites=self.get_satellites_payload(), links=self.links_data)


if __name__ == "__main__":
    from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
    from data_integration_pipeline.io.file_reader import S3FileReader
    from data_integration_pipeline.settings import DATA_BUCKET

    for s3_path in [
        "silver/business_entity_registry/business_entity_registry.parquet",
        "silver/licenses_registry/licenses_registry.parquet",
        "silver/sub_contractors_registry/sub_contractors_registry.parquet",
    ]:
        data_model = ModelMapper.get_data_model(s3_path)
        reader = S3FileReader(s3_path=s3_path, bucket_name=DATA_BUCKET)
        for raw_record in reader:
            print("#####################################", s3_path)
            record = data_model(**raw_record)
            vault_payload: VaultedRecordPayload = RecordToVaultConverter(record=record).run()
            for table_type, tables in vault_payload.items():
                for table_name, table in tables.items():
                    print(table_type, table_name, table)
            break
