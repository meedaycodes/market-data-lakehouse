"""
### daily_bars

End-of-day bars for one trading date, every weekday at 18:30 New York time.

```
refresh_security_master ─┬─► bronze_alpaca ─┬─► build_silver ─► check_completeness
                         └─► bronze_yahoo  ─┘
                       (any failure anywhere) ─► fail_if_any_task_failed
```

Every task is a container from the `market-lakehouse` image running one of
the project's own CLIs -- the same commands you can run by hand. Airflow
decides what runs and when; it never imports pyspark or `ingestion`.

**Manual runs:** trigger with config `{"trade_date": "YYYY-MM-DD"}`. Without
it, a run triggered before 17:00 New York is refused rather than loading a
half-finished bar.

Design: `docs/phase-2-design.md`.
"""

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Param, TriggerRule, task
from airflow.timetables.trigger import CronTriggerTimetable
from docker.types import Mount

from daily_bars_dates import trade_date_for

# Passed to task containers as private environment: Docker receives them,
# but Airflow never renders them into logs or the UI.
SECRET_ENV = ["SEC_CONTACT_EMAIL", "ALPACA_API_KEY", "ALPACA_SECRET_KEY", "OPENFIGI_API_KEY"]

TRADE_DATE = "{{ trade_date_for(params.trade_date, logical_date, dag_run.run_after) }}"
WINDOW = f"--start {TRADE_DATE} --end {TRADE_DATE}"


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set -- it comes from docker-compose.yml's airflow services")
    return value


def lakehouse_task(task_id: str, command: str, **kwargs) -> DockerOperator:
    """A task that runs `command` in a fresh lakehouse container."""
    return DockerOperator(
        task_id=task_id,
        image=_env("LAKEHOUSE_IMAGE"),
        command=command,
        docker_url=_env("LAKEHOUSE_DOCKER_URL"),
        # The HOST path: the Docker daemon resolves bind mounts on the host,
        # not inside the Airflow container that asks for them.
        mounts=[Mount(source=_env("LAKEHOUSE_HOST_DIR"), target="/home/jovyan/work", type="bind")],
        private_environment={k: os.environ[k] for k in SECRET_ENV if os.environ.get(k)},
        # DockerOperator would otherwise bind-mount a temp dir from inside the
        # Airflow container -- a path the host daemon can't see.
        mount_tmp_dir=False,
        auto_remove="success",  # keep failed containers around to inspect
        do_xcom_push=False,  # don't store log lines in Airflow's database
        **kwargs,
    )


with DAG(
    dag_id="daily_bars",
    # New York time, set explicitly: the market's clock, not the server's
    # (UTC) or the developer's (London). Airflow converts to UTC correctly
    # across both countries' daylight-saving changes.
    schedule=CronTriggerTimetable("30 18 * * 1-5", timezone="America/New_York"),
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,  # history comes from ingestion.bars.backfill, not replayed runs
    # One run at a time: two runs MERGEing into the same Delta table would
    # conflict.
    max_active_runs=1,
    user_defined_macros={"trade_date_for": trade_date_for},
    params={
        "trade_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Trading date to load (YYYY-MM-DD). Leave empty for scheduled runs.",
        )
    },
    default_args={"owner": "lakehouse", "retries": 0, "execution_timeout": timedelta(minutes=30)},
    tags=["phase-2", "bars"],
    doc_md=__doc__,
) as dag:
    master = lakehouse_task(
        "refresh_security_master",
        "python -m ingestion.build_security_master",
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # ALL_DONE: fetch even if the master refresh failed. It exits 1 when one
    # security is quarantined, but it still wrote the other 79 -- and an
    # older master is far better than no bars.
    bronze = [
        lakehouse_task(
            f"bronze_{source}",
            f"python -m ingestion.bars.load_bronze --source {source} {WINDOW} --run-id {{{{ run_id }}}}",
            retries=2,
            retry_delay=timedelta(minutes=10),  # vendors hiccup; most recover in minutes
            trigger_rule=TriggerRule.ALL_DONE,
        )
        for source in ("alpaca", "yahoo")
    ]

    # ALL_DONE again: if one vendor failed, still build silver from the
    # other, and let the completeness check say exactly what's missing.
    silver = lakehouse_task(
        "build_silver", f"python -m ingestion.bars.build_silver {WINDOW}", trigger_rule=TriggerRule.ALL_DONE
    )
    complete = lakehouse_task(
        "check_completeness",
        f"python -m ingestion.bars.check_completeness {WINDOW}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def fail_if_any_task_failed():
        """Airflow judges a run by its final tasks. With ALL_DONE above, a
        failed master refresh would otherwise end in a green run. This task
        only runs when something failed, and fails the run when it does."""
        raise RuntimeError("An upstream task failed -- see the failed task's log.")

    watcher = fail_if_any_task_failed()

    master >> bronze >> silver >> complete
    [master, *bronze, silver, complete] >> watcher
