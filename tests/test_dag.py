"""DAG checks: it parses, has no cycles, retries are set, nothing is hard-coded.

The first three need Airflow (they run inside the container: `docker compose exec airflow-scheduler pytest /opt/airflow/tests`);
the rest run anywhere.
"""
import ast
import glob
import os
import re
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "include"))
DAG_DIR = os.path.join(ROOT, "dags")

try:
    import airflow  # noqa: F401
    HAVE_AIRFLOW = True
except ImportError:
    HAVE_AIRFLOW = False
needs_airflow = pytest.mark.skipif(not HAVE_AIRFLOW, reason="Airflow not installed here")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder=DAG_DIR)


@needs_airflow
def test_dags_parse(dagbag):
    assert dagbag.import_errors == {}
    assert "retail_daily" in dagbag.dags


@needs_airflow
def test_no_cycles(dagbag):
    from airflow.sdk.bases.operator import BaseOperator  # noqa: F401
    dag = dagbag.dags["retail_daily"]
    graph = {t.task_id: set(t.downstream_task_ids) for t in dag.tasks}
    seen, stack = set(), set()

    def visit(n):
        assert n not in stack, f"cycle through {n}"
        if n in seen:
            return
        stack.add(n)
        for m in graph[n]:
            visit(m)
        stack.discard(n)
        seen.add(n)
    for n in graph:
        visit(n)


@needs_airflow
def test_retries_set_and_single_active_run(dagbag):
    dag = dagbag.dags["retail_daily"]
    assert dag.max_active_runs == 1
    assert all((t.retries or 0) >= 1 for t in dag.tasks)


def test_step_graph_has_no_cycles_and_dims_before_facts():
    from pipeline_spec import GOLD, SILVER
    for steps in ([(n, d) for n, d in SILVER], [(n, d) for n, d, _ in GOLD]):
        deps = dict(steps)
        done = set()
        for _ in range(len(deps) + 1):                       # a cycle would never finish
            done |= {n for n, d in deps.items() if all(x in done for x in d)}
        assert done == set(deps), "cycle or missing dependency"
    gold = {n: d for n, d, _ in GOLD}
    for fact in ("fct_sales", "fct_inventory"):
        assert {"dim_item", "dim_locn", "dim_day"} <= set(gold[fact])


def test_every_step_has_a_sql_file():
    from pipeline_spec import GOLD, INIT_FILES, SILVER
    sql = os.path.join(ROOT, "include", "sql")
    for n, _ in SILVER:
        assert os.path.exists(os.path.join(sql, "silver", f"{n}.sql")), n
    for n, _, _ in GOLD:
        assert os.path.exists(os.path.join(sql, "gold", f"{n}.sql")), n
    for n in INIT_FILES:
        assert os.path.exists(os.path.join(sql, "init", f"{n}.sql")), n
    assert os.path.exists(os.path.join(sql, "recon", "checks.sql"))


def test_dag_files_have_no_hardcoded_credentials_paths_or_tables():
    bad = re.compile(r"(password|passwd|secret|token|account\s*=)|(/opt/airflow|/home/|[A-Za-z]:\\)|"
                     r"\b(stg_|silver_|dim_|fct_|rej_row|etl_run_log)", re.I)
    for path in glob.glob(os.path.join(DAG_DIR, "*.py")):
        ast.parse(open(path, encoding="utf-8").read())       # at least valid Python
        for n, line in enumerate(open(path, encoding="utf-8"), 1):
            code = line.split("#")[0]
            assert not bad.search(code), f"{os.path.basename(path)}:{n}: {line.strip()}"
