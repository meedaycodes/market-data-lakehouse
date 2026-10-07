"""
One place to build a Delta-enabled SparkSession, so every job gets the same
settings instead of a copy-pasted builder per script.

The session time zone is pinned to UTC. Spark renders and parses timestamps
in the session time zone, which defaults to the machine's -- so without
this, the same valid_from prints differently on a laptop in Lagos and a
cluster in us-east-1, and an as-of query written as a string literal means
a different instant on each. Market data spans time zones; store and
compare in UTC, convert only for display.

Workers run the driver's own Python. Spark executes Python code in separate
worker processes, and by default starts them with whatever `python3` is
first on PATH -- in a local .venv that can be a different Python entirely
(e.g. Homebrew's 3.14 beside the venv's 3.11), and PySpark refuses to mix
minor versions. Inside the container there's only one Python, which is why
it only shows up outside Docker.
"""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
