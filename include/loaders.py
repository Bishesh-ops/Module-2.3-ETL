import csv
import logging
import os
import tempfile
import uuid


class SqliteLoader:
    placeholder = "?"

    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur

    def insert(self, table, columns, rows):
        if not rows:
            return 0

        marks = ",".join("?" for _ in columns)
        self.conn.executemany(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({marks})",
            rows
        )
        return len(rows)

    def commit(self):
        self.conn.commit()


class SnowflakeLoader:
    placeholder = "%s"
    NULL_SENTINEL = "__AIRFLOW_SQL_NULL_7F31A9__"

    def __init__(self, conn, work_dir):
        self.conn = conn
        self.work_dir = work_dir

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params or None)
            return cur
        except Exception:
            logging.exception("SNOWFLAKE SQL FAILED: %s", sql)
            raise

    def commit(self):
        pass

    def insert(self, table, columns, rows):
        if not rows:
            return 0

        os.makedirs(self.work_dir, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            "w",
            newline="",
            suffix=".csv",
            dir=self.work_dir,
            delete=False,
            encoding="utf-8"
        ) as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_ALL)

            for row in rows:
                writer.writerow(
                    self.NULL_SENTINEL if value is None else value
                    for value in row
                )

            path = fh.name

        filename = os.path.basename(path)
        stage = f"@%{table}/load_{uuid.uuid4().hex}/"

        try:
            put_cur = self.execute(
                f"PUT file://{path} {stage} "
                "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )

            logging.getLogger("airflow.task").info(
                "SNOWFLAKE PUT RESULT: %s",
                put_cur.fetchall()
            )

            self.execute(
                f"COPY INTO {table} ({','.join(columns)}) "
                f"FROM {stage} "
                f"FILES = ('{filename}.gz') "
                "FILE_FORMAT = (TYPE = CSV "
                "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                "EMPTY_FIELD_AS_NULL = FALSE "
                f"NULL_IF = ('{self.NULL_SENTINEL}')) "
                "PURGE = TRUE ON_ERROR = ABORT_STATEMENT"
            )

        finally:
            if os.path.exists(path):
                os.remove(path)

        return len(rows)


def open_loader(conn_id=None):
    """A SnowflakeLoader built from the Airflow connection (no credentials in code)."""

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from config import get_setting

    hook = SnowflakeHook(
        snowflake_conn_id=conn_id or get_setting("SNOWFLAKE_CONN_ID")
    )

    return SnowflakeLoader(
        hook.get_conn(),
        get_setting("WORK_DIR")
    )
