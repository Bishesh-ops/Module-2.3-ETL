from files import FILES


def _stg(spec):
    cols = ",\n    ".join(f"{c} varchar(500)" for c in spec["columns"])
    return (f"CREATE TABLE IF NOT EXISTS {spec['table']} (\n    {cols},\n    batch_id varchar(10),\n"
            "    src_file varchar(200),\n    src_row_no integer\n)")


def stg_ddl():
    return [_stg(f) for f in FILES]


def ensure_stg(loader):
    for ddl in stg_ddl():
        loader.execute(ddl)
    loader.commit()
