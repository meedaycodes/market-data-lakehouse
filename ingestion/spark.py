"""
One place to build a Delta-enabled SparkSession, so every job gets the same
settings instead of a copy-pasted builder per script.

The session time zone is pinned to UTC. Spark renders and parses timestamps
in the session time zone, which defaults to the machine's -- so without
this, the same valid_from prints differently on a laptop in Lagos and a
cluster in us-east-1, and an as-of query written as a string literal means
a different instant on each. Market data spans time zones; store and
compare in UTC, convert only for display.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
