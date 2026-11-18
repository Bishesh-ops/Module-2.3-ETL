"""Runs the whole pipeline on SQLite so the SQL logic can be tested without Snowflake."""
import os, re, shutil, sqlite3, sys, tempfile
from datetime import date, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "include"))

import calendar_dims, ingest, init_db, loaders, recon, runner, sql_macros  # noqa: E402
from files import KEYS  # noqa: E402
from pipeline_spec import GOLD, INIT_FILES, SILVER  # noqa: E402

DATA = os.path.join(ROOT, "data", "incoming")
DAYS = [(date(2026, 9, 13) + timedelta(days=i)).isoformat() for i in range(5)]


def sqlite_ddl(text):
    out = []
    for line in text.splitlines():
        if line.strip().startswith("CONSTRAINT"):
            continue
        line = re.sub(r"integer\s+AUTOINCREMENT", "integer PRIMARY KEY AUTOINCREMENT", line).replace("DATE '9999-12-31'", "'9999-12-31'")
        out.append(line)
    return re.sub(r",(\s*)\)", r"\1)", "\n".join(out))


class Env:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        sql_macros.sqlite_functions(self.conn)
        self.loader = loaders.SqliteLoader(self.conn)
        self.bronze = tempfile.mkdtemp()

    def q(self, sql):
        return self.conn.execute(sql).fetchall()

    def init(self):
        for name in INIT_FILES:
            text = runner.render(f"init/{name}.sql", "1900-01-01", "sqlite")
            if name == "01_gold_tables":
                text = sqlite_ddl(text)
            for s in runner.split_statements(text):
                self.conn.execute(s)
        init_db.ensure_stg(self.loader)

    def step(self, rel, ds):
        runner.run_file(self.conn.execute, rel, ds, "sqlite", **calendar_dims.parts(ds))

    def ingest(self, ds):
        return [ingest.ingest_file(k, ds, self.loader, DATA, self.bronze) for k in KEYS]

    def silver(self, ds):
        for name, _ in SILVER:
            self.step(f"silver/{name}.sql", ds)

    def gold(self, ds):
        for name, _, _ in GOLD:
            self.step(f"gold/{name}.sql", ds)

    def reconcile(self, ds):
        return recon.run_recon(self.q, self.conn.execute, ds, "sqlite")

    def run_day(self, ds):
        self.ingest(ds); self.silver(ds); self.gold(ds)
        return self.reconcile(ds)

    def snapshot(self):
        names = [r[0] for r in self.q("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1")
                 if r[0] != "etl_run_log"]
        snap = {t: self.q(f"SELECT COUNT(*) FROM {t}")[0][0] for t in names}
        snap["net_sales"] = round(self.q("SELECT COALESCE(SUM(sale_amt),0) FROM fct_sales")[0][0], 2)
        snap["on_hand"] = self.q("SELECT COALESCE(SUM(on_hand_qty),0) FROM fct_inventory")[0][0]
        return snap
