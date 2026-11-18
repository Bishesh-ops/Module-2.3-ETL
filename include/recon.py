"""Reconciliation: run include/sql/recon/checks.sql, print every check, fail when Gold does not tie."""
import runner


class ReconciliationFailed(Exception):
    pass


def run_recon(query, execute, ds, dialect="snowflake", tol=0.01):
    stmts = runner.split_statements(runner.render("recon/checks.sql", ds, dialect))
    results = []
    for check, expected, actual in query(stmts[0]):
        e, a = float(expected or 0), float(actual or 0)
        results.append({"check": check, "expected": e, "actual": a, "status": "PASS" if abs(e - a) <= tol else "FAIL"})
    execute(f"DELETE FROM etl_run_log WHERE run_date = '{ds}' AND metric LIKE 'recon:%'")
    for r in results:
        execute("INSERT INTO etl_run_log (run_date, subject, metric, value_num) VALUES "
                f"('{ds}', '{r['check']}', 'recon:{r['status']}', {r['actual']})")
    failed = [r for r in results if r["status"] == "FAIL"]
    if failed:
        raise ReconciliationFailed("; ".join(f"{r['check']}: expected {r['expected']:.2f}, got {r['actual']:.2f}" for r in failed))
    return results


def reject_report(query, ds):
    return query(f"SELECT source, reject_cd, COUNT(*) FROM rej_row WHERE run_date = '{ds}' GROUP BY source, reject_cd ORDER BY 1, 2")
