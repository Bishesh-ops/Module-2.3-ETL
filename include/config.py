"""Settings. No credentials here: Snowflake login lives only in the Airflow connection.

Override any value with env ETL_<NAME> or an Airflow Variable etl_<name>.
"""
import os

_DEFAULTS = {
    "INCOMING_DIR": "/opt/airflow/data/incoming",   # where the four daily files arrive
    "BRONZE_ROOT": "/opt/airflow/data/bronze",      # verbatim copy of every file we load
    "WORK_DIR": "/tmp/etl_work",
    "SNOWFLAKE_CONN_ID": "snowflake_default",
    "SENSOR_TIMEOUT_SECONDS": "3600",
    "SENSOR_POKE_SECONDS": "60",
}


def get_setting(name, default=None):
    env = os.environ.get(f"ETL_{name}")
    if env is not None:
        return env
    try:  # only inside Airflow
        from airflow.sdk import Variable
        v = Variable.get(f"etl_{name.lower()}", default=None)
        if v is not None:
            return v
    except Exception:
        pass
    return default if default is not None else _DEFAULTS.get(name)
