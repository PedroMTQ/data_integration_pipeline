from typing import Iterable, Any
from itertools import islice
from data_integration_pipeline.core.entity_resolution.metadata import SplinkRunMetadata
from datetime import datetime


def batch_yielder(initial_yielder: Iterable[Any], batch_size: int) -> Iterable[list[Any]]:
    """Yield successive n-sized chunks from an iterable using islice."""
    it = iter(initial_yielder)
    while True:
        # creates a 'slice' of the iterator of length batch_size
        chunk = list(islice(it, batch_size))
        if not chunk:
            break
        yield chunk


def get_latest_metadata_by_table_group(metadata_list: list["SplinkRunMetadata"]) -> dict[str, "SplinkRunMetadata"]:
    """
    Groups runs by their input table combinations and returns only
    the most recent SplinkRunMetadata object for each group.
    """
    latest_runs: dict[str, SplinkRunMetadata] = {}
    for entry in metadata_list:
        # 1. Access attributes directly from the object
        tables = entry.inputs.get("table_names", [])
        table_group = ",".join(sorted(tables))
        # 2. Parse the timestamp string into a datetime for comparison
        # We assume entry.timestamp is an ISO format string
        current_ts = datetime.fromisoformat(entry.timestamp)

        if table_group not in latest_runs:
            # First time seeing this group? It's the current winner.
            latest_runs[table_group] = entry
        else:
            # Compare against the existing winner's timestamp
            existing_ts = datetime.fromisoformat(latest_runs[table_group].timestamp)
            if current_ts > existing_ts:
                latest_runs[table_group] = entry
    return list(latest_runs.values())
