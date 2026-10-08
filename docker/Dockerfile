# Spark + Delta Lake + Jupyter, all in one dev container. This is the
# "local Docker" side of the platform -- same Delta Lake table format and
# Spark APIs as a real Databricks workspace, zero cost, fully under your
# control. Point spark.jars.packages at the same delta-spark version on a
# real Databricks cluster and the write paths in this repo don't change.
#
# Pulled from quay.io: Jupyter stopped publishing to Docker Hub in 2023, and
# Docker Hub has no spark-3.5.1 tag. The Spark version here must match the
# pyspark pin in requirements.txt.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.1

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER ${NB_UID}

# Download the Delta Lake jars now, at build time, into ~/.ivy2. Otherwise
# configure_spark_with_delta_pip fetches them from Maven every time a new
# container starts -- and under Airflow every task is a new container, so
# every task would repeat the download and fail whenever Maven is
# unreachable. Baked in, a task starts with no network dependency on Maven.
RUN python -c "from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

WORKDIR /home/jovyan/work
ENV PYTHONPATH=/home/jovyan/work
