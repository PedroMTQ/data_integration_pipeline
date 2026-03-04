from __future__ import annotations

import argparse
from typing import Sequence

from data_integration_pipeline.io.logger import logger
from data_integration_pipeline.settings import CODE_VERSION, SERVICE_NAME


def _cmd_upload_bronze(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.upload_bronze import UploadBronzeJob

    UploadBronzeJob().run()


def _cmd_process_bronze(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.process_bronze_and_load_to_delta_silver import ProcessBronzetoSilver

    ProcessBronzetoSilver().run()


def _cmd_audit_silver(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.audit_silver import AuditSilverDataJob

    AuditSilverDataJob().run()


def _cmd_dedup_silver(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.deduplicate_silver_data import DeduplicateSilverDataJob

    DeduplicateSilverDataJob().run()


def _cmd_run_entity_resolution(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.run_entity_resolution import EntityResolutionJob

    EntityResolutionJob().run()


def _cmd_create_integrated(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.create_integrated_records import CreateIntegratedRecords

    CreateIntegratedRecords().run()


def _cmd_dedup_integrated(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.deduplicate_integrated_records import DeduplicateIntegratedRecordsJob

    DeduplicateIntegratedRecordsJob().run()


def _cmd_create_gold(_: argparse.Namespace) -> None:
    from data_integration_pipeline.jobs.create_gold_records import CreateGoldRecords

    CreateGoldRecords().run()


def _cmd_pipeline(_: argparse.Namespace) -> None:
    """
    Runs the full pipeline sequentially (useful for local demos / reviews).
    """
    _cmd_upload_bronze(argparse.Namespace())
    _cmd_process_bronze(argparse.Namespace())
    _cmd_audit_silver(argparse.Namespace())
    _cmd_dedup_silver(argparse.Namespace())
    _cmd_run_entity_resolution(argparse.Namespace())
    _cmd_create_integrated(argparse.Namespace())
    _cmd_dedup_integrated(argparse.Namespace())
    _cmd_create_gold(argparse.Namespace())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='dip', description=f'{SERVICE_NAME} CLI')
    parser.add_argument('--version', action='version', version=f'{SERVICE_NAME} {CODE_VERSION}')

    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('upload-bronze', help='Upload test data from tests/data/ to S3 bronze/').set_defaults(func=_cmd_upload_bronze)
    sub.add_parser('process-bronze', help='Process bronze files into silver Delta tables.').set_defaults(func=_cmd_process_bronze)
    sub.add_parser('audit-silver', help='Run Great Expectations audits on silver Delta tables.').set_defaults(func=_cmd_audit_silver)
    sub.add_parser('dedup-silver', help='Deduplicate silver Delta tables into deduplicated.parquet snapshots.').set_defaults(func=_cmd_dedup_silver)
    sub.add_parser('run-er', help='Run entity resolution (Splink) on deduplicated silver snapshots.').set_defaults(func=_cmd_run_entity_resolution)
    sub.add_parser('create-integrated', help='Create integrated records from the latest entity resolution run.').set_defaults(
        func=_cmd_create_integrated
    )
    sub.add_parser('dedup-integrated', help='Deduplicate integrated records.').set_defaults(func=_cmd_dedup_integrated)
    sub.add_parser('create-gold', help='Create gold records / data mart outputs.').set_defaults(func=_cmd_create_gold)
    sub.add_parser('pipeline', help='Run the full pipeline sequentially.').set_defaults(func=_cmd_pipeline)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger.info(f'Running CLI command: {args.command}')
    args.func(args)
    return 0
