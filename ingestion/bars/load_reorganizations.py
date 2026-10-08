"""
Load reference_data/reorganizations.py into silver/corporate_actions.

    python -m ingestion.bars.load_reorganizations

Rows get action_type 'reorg', source 'sec_edgar', and NULL value. Lineage
points at the evidence rather than at a pipeline run: source_ingested_at is
the filing's EDGAR acceptance time (when the fact became public) and
source_run_id is its accession number, so every row traces to a filing.

Idempotent, like every silver write: rerunning changes nothing; editing an
entry in the reference file (with a later accepted_at) revises it.
"""

import sys
from datetime import date, datetime

from pyspark.sql.types import DecimalType

from ingestion import paths
from ingestion.bars import silver
from reference_data.reorganizations import REORGANIZATIONS


def reorganization_rows(spark):
    rows = [
        (
            r["security_id"],
            date.fromisoformat(r["effective_date"]),
            "reorg",
            "sec_edgar",
            None,
            datetime.fromisoformat(r["accepted_at"].replace("Z", "+00:00")),
            r["accession"],
        )
        for r in REORGANIZATIONS
    ]
    schema = (
        "security_id string, ex_date date, action_type string, source string, "
        "value decimal(18,6), source_ingested_at timestamp, source_run_id string"
    )
    return spark.createDataFrame(rows, schema)


def main() -> int:
    from ingestion.spark import get_spark

    spark = get_spark("load-reorganizations")
    try:
        counts = silver.merge_actions(spark, paths.SILVER_CORPORATE_ACTIONS, reorganization_rows(spark))
        print(f"reorganizations: {counts['inserted']} inserted, {counts['revised']} revised")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
