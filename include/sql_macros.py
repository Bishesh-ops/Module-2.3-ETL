"""The few places Snowflake and SQLite (used only by the local tests) differ. SQL files call these macros."""
SNOWFLAKE = {
    "dt": lambda text: f"DATE '{text}'",
    "dadd": lambda expr, n: f"DATEADD(day, {n}, {expr})",
    "n": lambda col: f"TRY_TO_DECIMAL(NULLIF(TRIM({col}), ''), 18, 4)",
}
SQLITE = {
    "dt": lambda text: f"'{text}'",
    "dadd": lambda expr, n: f"date({expr}, '{int(n):+d} days')",
    "n": lambda col: f"pnum({col})",
}


def macros(dialect):
    m = dict(SNOWFLAKE if dialect == "snowflake" else SQLITE)
    n = m["n"]
    m["n0"] = lambda col: f"(CASE WHEN {col} IS NULL OR TRIM({col}) = '' THEN 0 ELSE {n(col)} END)"  # blank = 0
    return m


def sqlite_functions(conn):
    def pnum(x):
        try:
            t = (x or "").strip()
            return float(t) if t else None
        except ValueError:
            return None
    conn.create_function("pnum", 1, pnum)
