from dataclasses import dataclass, field

# ! These classes are not direct translations to datavault2.0 requirements, but they service as the building block for the record to vault processor


@dataclass
class Hub:
    name: str
    id_type: str


@dataclass
class Satellite:
    name: str
    hub_name: str = field(default=None)
    link_name: str = field(default=None)
    include_in_hash_diff: bool = field(default=True)

    def __post_init__(self):
        if not self.hub_name and not self.hub_name:
            raise Exception("Missing hub and link name")


@dataclass
class Link:
    local_hub_name: str
    remote_hub_name: str
    local_id_type: str
    remote_id_type: str
    name: str = field(init=False, default=None)
    metadata: dict = field(default=None)
    local_role: str = field(default="1")  # can also be used to define other links, e.g., child/parent
    remote_role: str = field(default="2")
    data_source: str = field(default=None)

    def __post_init__(self):
        sorted_hubs = sorted([self.local_hub_name, self.remote_hub_name])
        self.name = f"{sorted_hubs[0]}__to__{sorted_hubs[1]}"
