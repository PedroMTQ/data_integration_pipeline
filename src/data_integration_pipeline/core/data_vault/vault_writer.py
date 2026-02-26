import pyarrow as pa
from typing import Any
from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.io.delta_vault_client import DeltaVaultClient


class VaultWriter:
    def __init__(self, client: DeltaVaultClient):
        self.client = client

    def save_payload(self, vault_payload: dict[str, Any]):
        """
        Takes the output of RecordToVault.run() and persists it to Delta Lake.
        Handles Hubs, Links, and Satellites with their respective logic.
        """

        # 1. Save Hubs
        for table_name, records in vault_payload.get("hubs", {}).items():
            self._write_batch(table_name, records)

        # 2. Save Links
        for table_name, records in vault_payload.get("links", {}).items():
            self._write_batch(table_name, records)

        # 3. Save Satellites
        for table_name, record in vault_payload.get("satellites", {}).items():
            # Satellites from your run() are currently single dicts,
            # we wrap them in a list for the Arrow conversion.
            self._write_batch(table_name, [record])

    def _write_batch(self, table_name: str, records: list[dict[str, Any]]):
        """Converts a list of dicts to Arrow and sends to DeltaVaultClient."""
        if not records:
            return

        try:
            # Create Arrow Table from list of dictionaries
            table = pa.Table.from_pylist(records)

            # Use the DeltaVaultClient's logic-aware write method
            self.client.write(table_name=table_name, data=table)

        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")
            raise e
