"""Connection check only: runs one query through the Snowflake connection and logs who/where we are.
Trigger it by hand. It loads nothing and changes nothing."""
import pendulum

from airflow.sdk import dag, task, Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(
    dag_id="test_snowflake_connection",
    description="Check the Snowflake connection",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,                  # manual trigger only
    catchup=False,
    tags=["check", "snowflake"],
)
def test_snowflake_connection():
    @task
    def check_connection():
        conn_id = Variable.get("snowflake_connection", default="snowflake_default")
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        row = hook.get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        names = ["user", "role", "warehouse", "database", "schema"]
        info = dict(zip(names, row))
        for k, v in info.items():
            print(f"{k:10}: {v}")
        missing = [k for k, v in info.items() if not v]
        if missing:                  # a None here means the connection did not set it
            raise ValueError(f"Not set on the connection: {missing}")
        return info

    check_connection()


test_snowflake_connection()