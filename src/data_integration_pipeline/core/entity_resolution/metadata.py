from dataclasses import asdict, dataclass
from data_integration_pipeline.settings import (
    SERVICE_NAME,
    CODE_VERSION,
    SPLINK_CLUSTERING_THRESHOLD,
    SPLINK_INFERENCE_PREDICT_THRESHOLD,
    ENTITY_RESOLUTION_DATA_FOLDER,
    PARQUET_TABLE_SUFFIX,
)
import os
import sys
from splink import Linker
import json
import splink
from datetime import datetime, timezone
import hashlib


@dataclass
class SplinkRunMetadata:
    run_id: str
    links_s3_path: str
    timestamp: str
    execution_context: dict
    inputs: dict
    outputs: dict
    model_metadata: dict
    overlap_report: dict = None

    def __str__(self) -> str:
        def format_section(title, data):
            lines = [f'\n{title}:']
            for k, v in data.items():
                if isinstance(v, dict):
                    lines.append(f'  • {k.replace("_", " ").title()}:')
                    for sub_k, sub_v in v.items():
                        lines.append(f'    - {sub_k}: {sub_v}')
                elif isinstance(v, list):
                    lines.append(f'  • {k.replace("_", " ").title()}: {", ".join(map(str, v))}')
                else:
                    lines.append(f'  • {k.replace("_", " ").title()}: {v}')
            return '\n'.join(lines)

        return (
            f'\n{"─" * 60}'
            f'\n🚀 Run ID: {self.run_id}\n'
            f'📅 Time:    {self.timestamp}\n'
            f'📍 S3 Path: {self.links_s3_path}\n'
            f'{format_section("📥 Inputs", self.inputs)}\n'
            f'{format_section("📤 Outputs", self.outputs)}\n'
            f'{format_section("📤 Overlap Report", self.overlap_report)}\n'
            f'{format_section("⚙️  Model Details", self.model_metadata)}\n'
            f'{"─" * 60}\n'
        )

    @classmethod
    def from_splink(
        cls,
        run_id: str,
        links_s3_path: str,
        table_names: list[str],
        linker: Linker,
        links_count: int,
        clusters_count: int,
        records_count: dict[str, int],
        overlap_report: dict[str, int],
    ) -> 'SplinkRunMetadata':
        """
        Factory method to 'unpack' Splink objects into this metadata class.
        """
        # Stable Hash Logic (MD5 is better than hash() for cross-session stability)
        settings_dict = linker._settings_obj.as_dict()
        settings_json = json.dumps(settings_dict, sort_keys=True)
        settings_hash = hashlib.md5(settings_json.encode()).hexdigest()
        return cls(
            run_id=run_id,
            links_s3_path=links_s3_path,
            timestamp=datetime.now(timezone.utc).isoformat(),
            execution_context={
                f'{SERVICE_NAME}_version': CODE_VERSION,
                'python_version': sys.version.split()[0],
                'splink_version': splink.__version__,
            },
            inputs={'table_names': table_names, 'per_source_records_count': records_count, 'records_count': sum(records_count.values())},
            outputs={'links_count': links_count, 'clusters_count': clusters_count},
            model_metadata={
                'splink_inference_predict_threshold': SPLINK_INFERENCE_PREDICT_THRESHOLD,
                'splink_clustering_threshold': SPLINK_CLUSTERING_THRESHOLD,
                'settings_hash': settings_hash,
            },
            overlap_report=overlap_report,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def linkage_rate(self) -> float:
        total_in = self.inputs['records_count']
        if total_in == 0:
            return 0.0
        return (total_in - self.outputs['clusters_count']) / total_in

    @property
    def integrated_records_s3_path(self) -> float:
        return os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id, f'integrated_records{PARQUET_TABLE_SUFFIX}')

    @property
    def deduplicated_records_s3_path(self) -> float:
        return os.path.join(ENTITY_RESOLUTION_DATA_FOLDER, self.run_id, f'dedup_integrated_records{PARQUET_TABLE_SUFFIX}')
