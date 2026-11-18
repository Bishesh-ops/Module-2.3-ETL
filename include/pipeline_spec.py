"""Run order. Each step is a SQL file; deps say what must finish first. No table names here."""
INIT_FILES = ["01_gold_tables", "02_work_tables", "03_seeds"]
SILVER = [   # (sql file, needs)
    ("item", []), ("store", []), ("sales", ["item", "store"]), ("inventory", ["item", "store"]), ("quarantine", ["sales", "inventory"]),
]
GOLD = [     # dimensions first, then facts
    ("dim_year", [], "calendar"), ("dim_month", ["dim_year"], "calendar"), ("dim_week", ["dim_year"], "calendar"),
    ("dim_day", ["dim_month", "dim_week"], "calendar"),
    ("dim_subclass", [], "sql"), ("dim_item", ["dim_subclass"], "sql"),
    ("dim_district", [], "sql"), ("dim_locn", ["dim_district"], "sql"),
    ("fct_sales", ["dim_item", "dim_locn", "dim_day"], "sql"), ("fct_inventory", ["dim_item", "dim_locn", "dim_day"], "sql"),
]
