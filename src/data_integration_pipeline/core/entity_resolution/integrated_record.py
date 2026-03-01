from __future__ import annotations
from typing import Any, Optional, ClassVar
from collections import Counter

from pydantic import (
    Field,
    model_serializer,
    model_validator,
    BaseModel,
)
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.settings import (
    CLUSTER_ID_STR,
    COMPOSITE_ID_STR,
    LDTS_COLUMN,
    HASH_DIFF_COLUMN,
    LINK_RECORD_CONSENSUS_SCORE_STR,
    LINK_RECORD_DEPTH_SCORE_STR,
    LINK_RECORD_GLOBAL_SCORE_STR,
    DATA_SOURCE_STR,
    LINK_RECORD_ANCHOR_AGREEMENT_SCORE_STR,
    LINK_RECORD_IS_PRIMARY_STR,
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

class ModelEntityID(BaseModel):
    model_config = BASE_CONFIG_DICT
    entity_id: str = Field(description="Entity UEI")
    data_source: str = Field(description='Data source of the entity')
    depth_score: float = Field(description='Depth score of the entity')
    consensus_score: float = Field(description='Consensus score of the entity')
    global_score: float = Field(description='Global score of the entity')
    agreement_score: float = Field(description='Anchor agreement score of the entity')
    vendor_id: Optional[str] = Field(default=None, description="Vendor entity ID")

class SchemaRecord(BaseSchema):
    anchor_entity: ModelEntityID = Field(description="Anchor Entity UEI")
    alt_entities: list[ModelEntityID] = Field(description="List of non-anchor entities within the cluster")
    splink_id: Optional[str] = Field(description="Splink ID if sourced from links. This is transient and is newly derived in each Splink run and is used exclusively for auditing the ER process")
    has_id_conflict: bool = Field(default=False)

    company_name: str = Field(description="Entity name")
    address_1: Optional[str] = Field(default=None)
    postal_code: Optional[str] = Field(default=None)
    city: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    naics_code: Optional[str] = Field(default=None)
    naics_code_label: Optional[str] = Field(default=None)
    licenses: Optional[list[ModelLicense]] = Field(default=None)
    certification_type: Optional[str] = Field(default=None)
    trade_specialty: Optional[str] = Field(default=None)



class Record(BaseRecord):
    """
    Each integrated record is derived from one or more profiles.
    A profile can represent multiple business entities/sub contractors which can in turn have multiple licenses
    We define the integrated entity id by checking what is the entity with the highest level of agreement and depth on a cluster.

    Note that the entities are transitory, and each time we perform new linking, they can change.
    For that we need to do this in 3 steps:
    - create integrated records
    - create a id bridge mapping each id to their parent (or itself if the record is the parent)
    - create a view with the final gold record (for the downstream user)

    """

    _metadata_keys: ClassVar[set[str]] = {
        CLUSTER_ID_STR,
        COMPOSITE_ID_STR,
        LDTS_COLUMN,
        HASH_DIFF_COLUMN,
        LINK_RECORD_CONSENSUS_SCORE_STR,
        LINK_RECORD_DEPTH_SCORE_STR,
        LINK_RECORD_GLOBAL_SCORE_STR,
        LINK_RECORD_IS_PRIMARY_STR,
        LINK_RECORD_ANCHOR_AGREEMENT_SCORE_STR,
    }
    _record_schema: ClassVar[BaseSchema] = SchemaRecord
    _anchor_data_sources: ClassVar[set[str]] = {"business_entity_registry",'sub_contractors_registry'}
    _data_source: ClassVar[str] = "gold_business_entity"
    _primary_key: ClassVar[str] = "entity_id"
    _partition_key: ClassVar[str] = "city"

    # entity integration information
    anchor_entity: ModelEntityID = Field(description="Anchor Entity UEI")
    alt_entities: list[ModelEntityID] = Field(description="List of non-anchor entities within the cluster")
    splink_id: Optional[str] = Field(description="Splink ID if sourced from links. This is transient and is newly derived in each Splink run and is used exclusively for auditing the ER process")
    has_id_conflict: bool = Field(default=False)

    # survivorship fields
    company_name: str = Field(description="Entity name")
    address_1: Optional[str] = Field(default=None)
    postal_code: Optional[str] = Field(default=None)
    city: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    naics_code: Optional[str] = Field(default=None)
    naics_code_label: Optional[str] = Field(default=None)
    licenses: Optional[list[ModelLicense]] = Field(default=None)
    certification_type: Optional[str] = Field(default=None)
    trade_specialty: Optional[str] = Field(default=None)

    @staticmethod
    def calculate_global_consensus(record: dict, num_records: int, agreement_map: dict) -> float:
        """
        Calculates consensus as a ratio:
        (Sum of agreements with others) / (Maximum possible agreements)
        """
        num_other_records = num_records - 1
        if num_other_records <= 1:
            return 1.0
        total_agreement_points = 0
        potential_points = 0
        for key, val in record.items():
            if key not in Record._metadata_keys and val is not None and str(val).strip() != "":
                normalized_val = str(val).strip().upper()
                others_count = agreement_map.get(key, {}).get(normalized_val, 0) - 1
                total_agreement_points += max(0, others_count)
                potential_points += num_other_records

        return total_agreement_points / potential_points if potential_points > 0 else 0.0

    @staticmethod
    def calculate_completeness(record: dict, valid_data_keys: set[str]) -> float:
        """
        Counts how many fields in this specific record are populated.
        """
        if not valid_data_keys:
            return 0.0
        populated_count = sum(1 for key in valid_data_keys if record.get(key) is not None and str(record.get(key)).strip() != "")
        return populated_count / len(valid_data_keys)

    @staticmethod
    def get_data_keys(list_records: list[dict]) -> set[str]:
        """Identifies all keys in the cluster that are not internal metadata."""
        all_keys = set().union(*(d.keys() for d in list_records))
        return all_keys - Record._metadata_keys

    @staticmethod
    def rank_and_select_anchor(list_records: list[dict]) -> dict:
        data_keys = Record.get_data_keys(list_records)
        # gets per-field and per-field value count
        agreement_map = {field: Counter([str(record[field]).strip().upper() for record in list_records if record.get(field)])for field in data_keys}
        num_records = len(list_records)
        for record in list_records:
            record[LINK_RECORD_DEPTH_SCORE_STR] = Record.calculate_completeness(record=record, valid_data_keys=data_keys)
            # Consensus = sum of agreements from peers
            record[LINK_RECORD_CONSENSUS_SCORE_STR] = Record.calculate_global_consensus(record=record, num_records=num_records, agreement_map=agreement_map)
            # Total score
            record[LINK_RECORD_GLOBAL_SCORE_STR] = (record[LINK_RECORD_DEPTH_SCORE_STR] + 2 * record[LINK_RECORD_CONSENSUS_SCORE_STR]) / 3
        # we define an anchor record since these correspond to actual business entities
        valid_anchor_records = [r for r in list_records if r.get(DATA_SOURCE_STR) in Record._anchor_data_sources and r.get(Record._primary_key)]
        # if there are no anchor records, we assume it's not a valid record
        # ! this obviously depends on business-specific decisions
        if not valid_anchor_records:
            raise Exception('Missing anchor, dropping record...')
        anchor = max(valid_anchor_records, key=lambda x: (x[LINK_RECORD_GLOBAL_SCORE_STR]))
        anchor[LINK_RECORD_IS_PRIMARY_STR] = True
        return anchor

    @staticmethod
    def calculate_anchor_agreement(record: dict, anchor: dict, data_keys: set[str]) -> float:
        """Calculates 0.0-1.0 agreement between a record and the chosen primary anchor."""
        agree_count = 0
        compare_count = 0
        for k in data_keys:
            r_val = record.get(k)
            a_val = anchor.get(k)
            if r_val not in (None, "") and a_val not in (None, ""):
                compare_count += 1
                if str(r_val).strip().upper() == str(a_val).strip().upper():
                    agree_count += 1
        return agree_count / compare_count if compare_count > 0 else 0.0

    def get_collapsed_dict(anchor: dict, sorted_records: list[dict]) -> dict:
        anchor_entity = ModelEntityID(entity_id=anchor[Record._primary_key],
                                      data_source=anchor[DATA_SOURCE_STR],
                                      depth_score=anchor[LINK_RECORD_DEPTH_SCORE_STR],
                                      consensus_score=anchor[LINK_RECORD_CONSENSUS_SCORE_STR],
                                      global_score=anchor[LINK_RECORD_GLOBAL_SCORE_STR],
                                      # since it's the anchor
                                      agreement_score=1.0,
                                      )
        alt_entities = []
        for record in sorted_records:
            if record.get(Record._primary_key):
                alt_entity = ModelEntityID(entity_id=record[Record._primary_key],
                                           data_source=record[DATA_SOURCE_STR],
                                           depth_score=record[LINK_RECORD_DEPTH_SCORE_STR],
                                           consensus_score=record[LINK_RECORD_CONSENSUS_SCORE_STR],
                                           global_score=record[LINK_RECORD_GLOBAL_SCORE_STR],
                                           agreement_score=record[LINK_RECORD_ANCHOR_AGREEMENT_SCORE_STR],
                                           vendor_id=record.get('vendor_id'),
                                           )
                alt_entities.append(alt_entity)
        res = {
            "anchor_entity": anchor_entity,
            "alt_entities": alt_entities,
            "splink_id": anchor[CLUSTER_ID_STR],
            "licenses": []
        }

        data_keys = Record.get_data_keys(sorted_records)
        licenses = []
        seen_license_ids = set()
        filled_keys = set()
        fields_to_reject = {'license_id', 'expiration_date'}
        for record in sorted_records:
            # A. Aggregation: Collect all unique licenses across the cluster
            if record[DATA_SOURCE_STR] == ModelLicense._data_source:
                license_id = record.get('license_id')
                if license_id and license_id not in seen_license_ids:
                    licenses.append(ModelLicense(license_id=license_id, expiration_date=record.get('expiration_date')))
                    seen_license_ids.add(license_id)
            # B. Survivorship: Fill fields from highest-ranked record first
            # TODO improve survivorship rules, potentially on a per-field basis
            for k in data_keys:
                if k not in filled_keys and k not in fields_to_reject:
                    record_value = record.get(k)
                    if record_value not in (None, ""):
                        res[k] = record_value
                        filled_keys.add(k)
        res['licenses'] = licenses
        return res

    @classmethod
    def from_cluster(cls, list_records: list[dict]) -> "Record":
        """
        receives cluster data from multiple data sources, including multiple records from the same data source
        We don't really do any data validation here, just survivorship
        e.g.:
        [{'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': 'UEI1000', 'company_name': 'LIBERTY HVAC SOLUTIONS LLC', 'company_name_normalized': 'LIBERTY HVAC SOLUTIONS', 'address_1': '3555 COMMERCE WAY', 'postal_code': '33101', 'city': 'MIAMI', 'is_active': True, 'ldts': datetime.datetime(2026, 2, 28, 14, 37, 21, 556764, tzinfo=<DstTzInfo 'Europe/Luxembourg' CET+1:00:00 STD>), 'composite_id': 'entity_id-__-UEI1000', 'license_id': None, 'naics_code': None, 'naics_code_label': None, 'expiration_date': None, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}, {'cluster_id': 'business_entity_registry-__-UEI1000', 'entity_id': None, 'company_name': 'Liberty Climate Pro', 'company_name_normalized': 'LIBERTY CLIMATE PRO', 'address_1': '3555 COMMERCE WAY', 'postal_code': None, 'city': 'PHOENIX', 'is_active': None, 'ldts': datetime.datetime(2026, 2, 28, 14, 37, 22, 157134, tzinfo=<DstTzInfo 'Europe/Luxembourg' CET+1:00:00 STD>), 'composite_id': 'license_id-__-ST1000', 'license_id': 'ST1000', 'naics_code': None, 'naics_code_label': None, 'expiration_date': {'year': 2026, 'month': 9, 'day': 14}, 'vendor_id': None, 'certification_type': None, 'trade_specialty': None}]

        """
        if not list_records:
            raise ValueError("Cannot process an empty cluster")
        anchor: dict = cls.rank_and_select_anchor(list_records=list_records)
        if not anchor:
            raise Exception(f'Anchor record not found for {list_records}')
        data_keys = Record.get_data_keys(list_records)
        for record in list_records:
            record[LINK_RECORD_ANCHOR_AGREEMENT_SCORE_STR] = cls.calculate_anchor_agreement(record=record, anchor=anchor, data_keys=data_keys)
        # Sort for survivorship: Anchor first, then highest global score
        sorted_records = sorted(list_records, key=lambda x: (x.get(LINK_RECORD_IS_PRIMARY_STR, False), x[LINK_RECORD_GLOBAL_SCORE_STR],),reverse=True)
        record_data = cls.get_collapsed_dict(anchor=anchor, sorted_records=sorted_records)
        return cls(**record_data)

    @model_validator(mode="after")
    def validate_id_consistency(self) -> "Record":
        """
        Critical check: If we have multiple IDs, we must flag it for the business.
        """
        if len(self.alt_entities) > 0:
            self.has_id_conflict = True
            # TODO change to warning
            logger.debug(f"Cluster {self.splink_id} has ID conflict: {self.anchor_entity} vs {self.alt_entities}")
        return self

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict[str, Any]:
        """
        Serializes the integrated record into a flat dictionary with 
        nested dictionaries for Struct/List conversion in PyArrow.
        """
        return {
            "splink_id": self.splink_id,
            "has_id_conflict": self.has_id_conflict,
            "anchor_entity": self.anchor_entity.model_dump(),
            "alt_entities": [entity.model_dump() for entity in self.alt_entities] if self.alt_entities else None,

            "company_name": self.company_name,
            "address_1": self.address_1,
            "postal_code": self.postal_code,
            "city": self.city,
            "is_active": self.is_active,

            "naics_code": self.naics_code,
            "naics_code_label": self.naics_code_label,
            "certification_type": self.certification_type,
            "trade_specialty": self.trade_specialty,

            "licenses": [lic.model_dump() for lic in self.licenses] if self.licenses else None,
        }


if __name__ == "__main__":
    from datetime import datetime

    cluster_data = [
        {
            "cluster_id": "business_entity_registry-__-UEI1000",
            "entity_id": "UEI1000",
            "company_name": "LIBERTY HVAC SOLUTIONS LLC",
            "company_name_normalized": "LIBERTY HVAC SOLUTIONS",
            "address_1": "3555 COMMERCE WAY",
            "postal_code": "33101",
            "city": "MIAMI",
            "is_active": True,
            "ldts": datetime.now(),
            "composite_id": "entity_id-__-UEI1000",
            "data_source": "business_entity_registry",
            "license_id": None,
            "naics_code": None,
            "naics_code_label": None,
            "expiration_date": None,
            "vendor_id": None,
            "certification_type": None,
            "trade_specialty": None,
        },
        {
            "cluster_id": "business_entity_registry-__-UEI1000",
            "entity_id": "UEI10510",
            "company_name": "LIBERTY HVAC LLC",
            "company_name_normalized": "LIBERTY HVAC",
            "address_1": None,
            "postal_code": None,
            "city": "MIAMI",
            "is_active": True,
            "ldts": datetime.now(),
            "composite_id": "entity_id-__-UEI1000",
            "data_source": "business_entity_registry",
            "license_id": None,
            "naics_code": None,
            "naics_code_label": None,
            "expiration_date": None,
            "vendor_id": None,
            "certification_type": None,
            "trade_specialty": None,
        },
        {
            "cluster_id": "business_entity_registry-__-UEI1000",
            "entity_id": None,
            "company_name": "Liberty Climate Pro",
            "company_name_normalized": "LIBERTY CLIMATE PRO",
            "address_1": "3555 COMMERCE WAY",
            "postal_code": None,
            "city": "PHOENIX",
            "is_active": None,
            "ldts": datetime.now(),
            "composite_id": "license_id-__-ST1000",
            "data_source": "licenses_registry",
            "license_id": "ST1000",
            "naics_code": None,
            "naics_code_label": None,
            "expiration_date": {"year": 2026, "month": 9, "day": 14},
            "vendor_id": None,
            "certification_type": None,
            "trade_specialty": None,
        },
    ]
    record = Record(**{"data": cluster_data})
    print(record)
    print(record.model_dump())