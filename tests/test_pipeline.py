"""Pipeline logic tests on SQLite (same SQL files as Snowflake)."""
import csv, os

import harness as H


def source_net(ds):
    with open(os.path.join(H.DATA, f"pos_sales_{ds.replace('-', '')}.csv")) as fh:
        return round(sum(float(r["sale_amt"] or 0) for r in csv.DictReader(fh)), 2)


def run_all():
    e = H.Env(); e.init()
    for ds in H.DAYS:
        e.run_day(ds)                        # raises if reconciliation fails
    return e


def test_five_day_backfill_reconciles_and_net_sales_tie_to_files():
    e = run_all()
    for ds in H.DAYS:
        gold = e.q(f"SELECT COALESCE(SUM(f.sale_amt),0) FROM fct_sales f JOIN dim_day d ON d.day_key=f.day_key WHERE d.day_dt='{ds}'")[0][0]
        rej = e.q(f"SELECT COALESCE(SUM(amount),0) FROM rej_row WHERE run_date='{ds}' AND source='pos_sales'")[0][0]
        assert round(gold + rej, 2) == source_net(ds)


def test_second_run_changes_nothing():
    e = run_all()
    before = e.snapshot()
    for ds in H.DAYS:                        # the whole backfill again, in order
        e.run_day(ds)
    assert e.snapshot() == before
    e.run_day(H.DAYS[2])                     # one earlier day again
    assert e.snapshot() == before


def test_day3_defects_are_quarantined_and_counted_and_run_continues():
    e = run_all()
    got = dict(e.q("SELECT reject_cd, COUNT(*) FROM rej_row WHERE run_date='2026-09-15' AND source='pos_sales' GROUP BY reject_cd"))
    assert got == {"NULL_ITEM": 2, "NEGATIVE_QTY": 1, "ORPHAN_ITEM": 1, "DUPLICATE": 1}
    assert e.q("SELECT COUNT(*) FROM rej_row WHERE run_date <> '2026-09-15'")[0][0] == 0


def test_store_district_change_is_scd2():
    e = run_all()
    rows = e.q("SELECT d.district_id, l.effective_start_dt, l.effective_end_dt, l.active_flg FROM dim_locn l JOIN dim_district d ON d.district_key=l.district_key WHERE l.locn_id='1004' ORDER BY l.effective_start_dt")
    assert [r[0] for r in rows] == ["D2", "D3"] and rows[0][2] == "2026-09-14" and rows[1][1] == "2026-09-15" and rows[1][2] == "9999-12-31"
    # sales of 09-14 use the old version, sales of 09-16 the new one
    for ds, dist in (("2026-09-14", "D2"), ("2026-09-16", "D3")):
        assert e.q(f"SELECT COUNT(DISTINCT dd.district_id) FROM fct_sales f JOIN dim_day d ON d.day_key=f.day_key JOIN dim_locn l ON l.locn_key=f.locn_key JOIN dim_district dd ON dd.district_key=l.district_key WHERE d.day_dt='{ds}' AND l.locn_id='1004'")[0][0] == 1
        assert e.q(f"SELECT dd.district_id FROM fct_sales f JOIN dim_day d ON d.day_key=f.day_key JOIN dim_locn l ON l.locn_key=f.locn_key JOIN dim_district dd ON dd.district_key=l.district_key WHERE d.day_dt='{ds}' AND l.locn_id='1004' LIMIT 1")[0][0] == dist


def test_item_change_is_scd2_with_generated_keys():
    e = run_all()
    rows = e.q("SELECT i.item_key, s.subclass_id, i.effective_start_dt, i.effective_end_dt FROM dim_item i JOIN dim_subclass s ON s.subclass_key=i.subclass_key WHERE i.item_id='I007' ORDER BY i.item_key")
    assert [r[1] for r in rows] == ["SC2", "SC3"] and rows[0][0] != rows[1][0] and rows[0][3] == "2026-09-15" and rows[1][2] == "2026-09-16"


def test_scd1_subclass_name_updated_in_place():
    e = run_all()
    assert e.q("SELECT COUNT(*), MAX(subclass_nm) FROM dim_subclass WHERE subclass_id='SC2'") == [(1, "Drinks & More")]


def test_inventory_rows_are_closed_never_deleted():
    e = run_all()
    rows = e.q("SELECT f.effective_start_dt, f.effective_end_dt FROM fct_inventory f JOIN dim_item i ON i.item_key=f.item_key JOIN dim_locn l ON l.locn_key=f.locn_key WHERE i.item_id='I001' AND l.locn_id='1001' ORDER BY 1")
    assert rows and all(r[1] != "9999-12-31" for r in rows)            # position vanished on 09-16: every row is closed
    assert max(r[1] for r in rows) == "2026-09-15"
    assert e.q("SELECT COUNT(*) FROM fct_inventory WHERE effective_end_dt = '9999-12-31'")[0][0] == 58


def test_reconciliation_fails_when_gold_does_not_tie():
    import pytest, recon
    e = H.Env(); e.init(); e.run_day(H.DAYS[0])
    e.conn.execute("UPDATE fct_sales SET sale_amt = sale_amt + 1 WHERE rowid = (SELECT MIN(rowid) FROM fct_sales)")
    with pytest.raises(recon.ReconciliationFailed):
        e.reconcile(H.DAYS[0])
