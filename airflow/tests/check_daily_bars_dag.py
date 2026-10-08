"""
Structural check of the daily_bars DAG, run inside the Airflow container
(the Airflow image has no pytest, and the lakehouse image has no Airflow):

    docker compose --profile airflow exec -T airflow-scheduler python - < airflow/tests/check_daily_bars_dag.py

Catches the mistakes that don't fail loudly: a DAG that silently stops
loading, a dependency wired the wrong way, a schedule in the wrong zone.
"""

import sys
from datetime import datetime, timezone

from airflow.dag_processing.dagbag import DagBag

# Airflow's DAG processor puts the DAGs folder on sys.path, which is how
# daily_bars.py can import its helper module; a standalone script has to do
# the same.
sys.path.insert(0, "/opt/airflow/dags")
bag = DagBag(dag_folder="/opt/airflow/dags")
assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"

dag = bag.get_dag("daily_bars")
assert dag is not None, "daily_bars not found"

expected_upstream = {
    "refresh_security_master": set(),
    "bronze_alpaca": {"refresh_security_master"},
    "bronze_yahoo": {"refresh_security_master"},
    "build_silver": {"bronze_alpaca", "bronze_yahoo"},
    "check_completeness": {"build_silver"},
    "fail_if_any_task_failed": {
        "refresh_security_master", "bronze_alpaca", "bronze_yahoo", "build_silver", "check_completeness",
    },
}
actual_upstream = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
assert actual_upstream == expected_upstream, actual_upstream

rules = {t.task_id: getattr(t.trigger_rule, "value", t.trigger_rule) for t in dag.tasks}  # enum in Airflow 3
assert rules["fail_if_any_task_failed"] == "one_failed", rules
assert all(rules[t] == "all_done" for t in ["bronze_alpaca", "bronze_yahoo", "build_silver", "check_completeness"]), rules

assert dag.max_active_runs == 1
assert str(dag.timetable._timezone) == "America/New_York", dag.timetable._timezone

# The next scheduled run after a known moment, in UTC: 18:30 New York.
after_summer = dag.timetable.next_dagrun_info(
    last_automated_data_interval=None,
    restriction=__import__("airflow.timetables.base", fromlist=["TimeRestriction"]).TimeRestriction(
        earliest=datetime(2026, 10, 7, 12, tzinfo=timezone.utc), latest=None, catchup=True
    ),
)
assert after_summer.logical_date == datetime(2026, 10, 7, 22, 30, tzinfo=timezone.utc), after_summer.logical_date

print(f"daily_bars OK: {len(dag.tasks)} tasks, schedule {dag.timetable.summary} America/New_York, "
      f"next run after 2026-10-07 12:00 UTC is {after_summer.logical_date:%Y-%m-%d %H:%M} UTC")
