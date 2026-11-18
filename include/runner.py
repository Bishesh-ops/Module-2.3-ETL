"""Renders a SQL file (Jinja) for one run date and runs its statements on one connection."""
import os

import jinja2

import sql_macros

SQL_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
_ENV = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=False)


def render(rel_path, ds, dialect="snowflake", **params):
    with open(os.path.join(SQL_ROOT, rel_path), encoding="utf-8") as fh:
        text = fh.read()
    return _ENV.from_string(text).render(ds=str(ds), **sql_macros.macros(dialect), **params)


def split_statements(sql):
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run_file(execute, rel_path, ds, dialect="snowflake", **params):
    stmts = split_statements(render(rel_path, ds, dialect, **params))
    for s in stmts:
        execute(s)
    return len(stmts)
