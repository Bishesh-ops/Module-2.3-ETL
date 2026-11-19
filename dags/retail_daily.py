
import pendulum

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Param, TaskGroup, dag, task

import calendar_dims
import ingest
import init_db
import recon
import runner
from config import get_setting
from files import KEYS
from loaders import open_loader
from pipeline_spec import GOLD, INIT_FILES, SILVER


def get_ds(context):
    given = (context.get("params") or {}).get("run_date")

    if not given or str(given).strip().upper() == "AUTO":
        return context["logical_date"].strftime("%Y-%m-%d")

    return str(given).strip()


def _files_arrived(**context):
    missing = ingest.missing_files(
        get_ds(context),
        get_setting("INCOMING_DIR")
    )
    if missing:
        print(f"still waiting for: {missing}")
    return not missing


@dag(
    dag_id="retail_daily",
    start_date=pendulum.datetime(2026, 9, 13, tz="UTC"),
    end_date=pendulum.datetime(2026, 9, 17, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(seconds=30)
    },
   params={
        "run_date": Param(
            "AUTO",
            type="string",
            description="Use AUTO for the logical date, or enter YYYY-MM-DD"
        )
    },
    tags=["assignment-2.3"],
)
def retail_daily():
    # @task            
    # def debug_context(**context):
    #     print("Logical date:", context["logical_date"])
    #     print("DAG run conf:", context["dag_run"].conf)
    #     print("Params:", context["params"])
    @task
    def init_tables():
        loader = open_loader()
        for name in INIT_FILES:
            for stmt in runner.split_statements(
                runner.render(f"init/{name}.sql", "1900-01-01")
            ):
                loader.execute(stmt)
        init_db.ensure_stg(loader)


    wait = PythonSensor(
        task_id="wait_for_files",
        python_callable=_files_arrived,
        mode="reschedule",
        poke_interval=int(get_setting("SENSOR_POKE_SECONDS")),
        timeout=int(get_setting("SENSOR_TIMEOUT_SECONDS")),
    )

    @task
    def ingest_file(key: str, **context):
        """Bronze copy + load into the staging table. Replaces the day's batch, so a rerun is safe."""
        return ingest.ingest_file(
            key,
            get_ds(context),
            open_loader(),
            get_setting("INCOMING_DIR"),
            get_setting("BRONZE_ROOT")
        )

    def sql_task(task_id, rel_path):
        @task(task_id=task_id)
        def run(**context):
            ds = get_ds(context)
            n = runner.run_file(
                open_loader().execute,
                rel_path,
                ds,
                **calendar_dims.parts(ds)
            )
            print(f"{rel_path}: {n} statements for {ds}")

        return run()

    @task
    def reconcile(**context):
        """Fails the run when Gold does not tie to the source files."""
        ds = get_ds(context)
        loader = open_loader()
        query = lambda s: loader.execute(s).fetchall()  # noqa: E731

        for source, code, n in recon.reject_report(query, ds):
            print(f"quarantined  {source:20} {code:16} {n}")

        for r in recon.run_recon(query, loader.execute, ds):
            print(
                f"{r['status']:5} {r['check']:55} "
                f"expected {r['expected']:>12.2f}  "
                f"actual {r['actual']:>12.2f}"
            )

    ingested = ingest_file.expand(key=KEYS)

    # debug_context() >> 
    init_tables() >> wait >> ingested

    with TaskGroup("silver") as silver:
        s = {
            name: sql_task(f"clean_{name}", f"silver/{name}.sql")
            for name, _ in SILVER
        }
        for name, needs in SILVER:
            for dependency in needs:
                s[dependency] >> s[name]

    with TaskGroup("gold") as gold:
        g = {
            name: sql_task(name, f"gold/{name}.sql")
            for name, _, _ in GOLD
        }
        for name, needs, _ in GOLD:
            for dependency in needs:
                g[dependency] >> g[name]

    ingested >> silver >> gold >> reconcile()


retail_daily()

