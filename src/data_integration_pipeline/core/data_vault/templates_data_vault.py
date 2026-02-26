from typing import Any

# ! These classes are not direct translations to datavault2.0 requirements, but they service as the building block for the record to vault processor


class Hub:
    """
    Hubs are one of the core building blocks of a Data Vault. Hubs record a unique list of all business keys for a single entity. For example, a Hub may contain a list of all Customer IDs in the business.

    Structure:
    In general, Hubs consist of 4 columns, described below.
    - Primary Key (src_pk): A primary key (or surrogate key) which is usually a hashed representation of the natural key.
    - Natural Key / Business Key (src_nk): This is usually a formal identification for the record, such as a customer ID or order number. Usually called the business key because this value has meaning in business processes such as transactions and events.
    - Load date (src_ldts): A load date or load date timestamp. This identifies when the record was first loaded into the database.
    - Record Source (src_source): The source for the record. This can be a code which is assigned to a source name in an external lookup table, or a string directly naming the source system. (i.e. 1 from the staging section, which is the code for stg_customer)
    """

    name: str  # Physical table name: e.g., 'hub_companies'
    nk_field: str  # Schema field: e.g., 'entity_id'
    record_source: str  # System of record: e.g., 'entity'
    is_primary: bool = False

    def __init__(self, name: str, record_source: str, is_primary: bool = False):
        self.name: str = name
        self.record_source: str = record_source
        self.is_primary = is_primary
        if not self.name.startswith("hub_"):
            self.name = f"hub_{self.name}"


class Satellite:
    """
    Satellites contain point-in-time payload data related to their parent Hub or Link records. Satellites are where the concrete data for our business entities in the Hubs and Links, reside. Each Hub or Link record may have one or more child Satellite records, which form a history of changes to that Hubs or Link record as they happen.

    Structure:
    Each component of a Satellite is described below.
    - Primary Key (src_pk): A primary key (or surrogate key) which is usually a hashed representation of the natural key. For a Satellite, this should be the same as the corresponding Link or Hub PK, concatenated with the load timestamp.
    - Hashdiff (src_hashdiff): This is a concatenation of the payload (below) and the primary key. This allows us to detect changes in a record (much like a checksum). For example, if a customer changes their name, the hashdiff will change as a result of the payload changing.
    - Payload (src_payload): The payload consists of concrete data for an entity (e.g. A customer). This could be a name, a date of birth, nationality, age, gender or more. The payload will contain some or all of the concrete data for an entity, depending on the purpose of the satellite.
    - Load Date/Timestamp (src_ldts): A load date or load date timestamp. This identifies when the record was first loaded into the database.
    - Record Source (src_source): The source for the record. This can be a code which is assigned to a source name in an external lookup table, or a string directly naming the source system.

    """

    name: str
    parent_name: str
    include_in_hash_diff: bool

    def __init__(
        self, name: str, hub_name: str = None, link_name: str = None, include_in_hash_diff: bool = True
    ):
        self.name = name
        self.parent_name = hub_name or link_name  # Reference a Hub/Link source_name
        self.include_in_hash_diff = include_in_hash_diff
        if not hub_name and not hub_name:
            raise Exception("Missing hub and link name")
        if link_name:
            if not self.name.startswith("sat_lnk"):
                self.name = f"sat_lnk_{self.name}"
        else:
            if not self.name.startswith("sat_"):
                self.name = f"sat_{self.name}"


class Link:
    name: str  # e.g., 'lnk_company_hierarchy'
    local_hub_name: str  # e.g., 'companies'
    local_nk_field: str  # e.g., 'entity_id'
    remote_hub_name: str  # e.g., 'companies'
    remote_nk_fields: list[str]  # e.g., ['entity_id_global']
    record_source: str
    metadata: dict
    """
    Links are another fundamental component in a Data Vault, forming the core of the raw vault along with Hubs and Satellites.
    Links model an association or link, between two business keys. A good example would be a list of all Orders and the Customers associated with those orders, for the whole business.

    Structure
    - Primary Key (src_pk): A primary key (or surrogate key) which is usually a hashed representation of the natural key. For Links, we take the natural keys (prior to hashing) represented by the foreign key columns below and create a hash on a concatenation of them.
    - Foreign Keys (src_fk): Foreign keys referencing the primary key for each Hub referenced in the Link (2 or more depending on the number of Hubs referenced)
    - Load Date/Timestamp (src_ldts): A load date or load date timestamp. This identifies when the record was first loaded into the database.
    - Record Source (src_source): The source for the record. This can be a code which is assigned to a source name in an external lookup table, or a string directly naming the source system.
    """

    def __init__(
        self,
        name: str,
        local_hub_name: str,
        remote_hub_name: str,
        metadata: dict[str, Any] = None,
        local_role: str = "1",
        remote_role: str = "2",
    ):
        self.name = name
        self.local_hub_name = local_hub_name
        self.remote_hub_name = remote_hub_name
        self.metadata = metadata or {}
        self.local_role = local_role
        self.remote_role = remote_role
        if not self.name.startswith("link_"):
            self.name = f"link_{self.name}"
