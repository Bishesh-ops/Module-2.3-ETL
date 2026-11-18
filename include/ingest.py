
import csv
import os
import shutil

from files import BY_KEY, KEYS, file_name


class HeaderMismatch(Exception):
    pass


def path_for(key, ds, root):
    return os.path.join(root, file_name(key, ds))


def missing_files(ds, root):
    return [k for k in KEYS if not os.path.exists(path_for(k, ds, root))]


def ingest_file(key, ds, loader, root, bronze_root):
    spec = BY_KEY[key]
    ds = str(ds)

    path = path_for(key, ds, root)
    filename = os.path.basename(path)

    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    dest_dir = os.path.join(bronze_root, key, f"ds={ds}")
    os.makedirs(dest_dir, exist_ok=True)

    shutil.copyfile(path, os.path.join(dest_dir, filename))

    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    header = [h.strip().lower() for h in rows[0]] if rows else []

    if header != spec["columns"]:
        raise HeaderMismatch(
            f"{filename}: header {header} != expected {spec['columns']}"
        )

    good, bad = [], []

    for n, row in enumerate(rows[1:], start=2):
        if not any(c.strip() for c in row):
            continue

        if len(row) != len(spec["columns"]):
            bad.append((n, ",".join(row)[:150]))
            continue

        good.append(row + [ds, filename, n])

    ph = loader.placeholder
    table = spec["table"]

    # Replace the existing batch so reruns are idempotent.
    loader.execute(
        f"DELETE FROM {table} WHERE batch_id = {ph}",
        (ds,)
    )

    loader.insert(
        table,
        spec["columns"] + ["batch_id", "src_file", "src_row_no"],
        good
    )

    # Replace previous shape rejects for this feed and run date.
    loader.execute(
        f"""
        DELETE FROM rej_row
        WHERE run_date = {ph}
          AND source = {ph}
          AND reject_cd = 'BAD_ROW_SHAPE'
        """,
        (ds, key)
    )

    if bad:
        loader.insert(
            "rej_row",
            ["run_date", "source", "reject_cd", "src_key", "amount"],
            [
                [ds, key, "BAD_ROW_SHAPE", f"line {n}: {txt}", None]
                for n, txt in bad
            ]
        )

    # Replace the previous file row count for this feed and run date.
    loader.execute(
        f"""
        DELETE FROM etl_run_log
        WHERE run_date = {ph}
          AND subject = {ph}
          AND metric = 'file_rows'
        """,
        (ds, key)
    )

    loader.insert(
        "etl_run_log",
        ["run_date", "subject", "metric", "value_num"],
        [[ds, key, "file_rows", len(good) + len(bad)]]
    )

    loader.commit()

    return {
        "file": key,
        "rows_in_file": len(good) + len(bad),
        "loaded": len(good),
        "bad_shape": len(bad),
    }
