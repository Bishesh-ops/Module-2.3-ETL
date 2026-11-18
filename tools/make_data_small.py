"""Writes the four daily files for 2026-09-13 .. 2026-09-17 into data/incoming (small, fixed random seed).

Planted on purpose:
  09-14  subclass SC2 renamed                     -> SCD1 (update in place)
  09-15  store 1004 moves district D2 -> D3       -> SCD2 on dim_locn
  09-15  sales file: 2 null item ids, 1 negative qty, 1 item missing from the master, 1 duplicated line -> quarantine
  09-16  item I007 moves subclass SC1 -> SC3      -> SCD2 on dim_item
  09-16  two inventory positions disappear        -> rows closed, never deleted
"""
import csv, os, random, sys
from datetime import date, timedelta

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "incoming")
DAYS = [date(2026, 9, 13) + timedelta(days=i) for i in range(5)]
ITEMS = [(f"I{n:03d}", f"Item {n}", f"Description of item {n}", "EA", "SC1" if n <= 4 else "SC2" if n <= 8 else "SC3") for n in range(1, 13)]
PRICE = {it[0]: round(2 + 1.7 * i, 2) for i, it in enumerate(ITEMS)}
STORES = [("1001", "Store North", "D1"), ("1002", "Store South", "D1"), ("1003", "Store East", "D2"),
          ("1004", "Store West", "D2"), ("1005", "Store Central", "D3")]
SUB = {"SC1": "Snacks", "SC2": "Drinks", "SC3": "Household"}
DIST = {"D1": "District One", "D2": "District Two", "D3": "District Three"}


def write(name, header, rows):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(header); w.writerows(rows)


stock = {}
rng = random.Random(7)
for d in DAYS:
    ymd = d.strftime("%Y%m%d")
    # item master (daily snapshot)
    rows = []
    for iid, nm, desc, uom, sc in ITEMS:
        if iid == "I007" and d >= date(2026, 9, 16):
            sc = "SC3"
        rows.append([iid, nm, desc, uom, sc, SUB[sc] + (" & More" if sc == "SC2" and d >= date(2026, 9, 14) else "")])
    write(f"item_master_{ymd}.csv", ["item_id", "item_nm", "item_desc", "uom_cd", "subclass_id", "subclass_nm"], rows)
    # store master
    rows = []
    for sid, nm, dist in STORES:
        if sid == "1004" and d >= date(2026, 9, 15):
            dist = "D3"
        rows.append([sid, nm, dist, DIST[dist], "USD"])
    write(f"store_master_{ymd}.csv", ["store_id", "store_nm", "district_id", "district_nm", "currency_cd"], rows)
    # sales
    sales, n = [], 0
    for sid, _, _ in STORES:
        for t in range(1, 5):
            for line in range(1, rng.randint(1, 3) + 1):
                iid = rng.choice(ITEMS)[0]; qty = rng.randint(1, 4)
                gross = round(qty * PRICE[iid], 2); disc = round(gross * 0.1, 2) if rng.random() < 0.2 else 0.0
                sales.append([f"S{ymd[4:]}{sid[-2:]}{t}", line, sid, iid, qty, round(gross - disc, 2), disc, round((gross - disc) * 0.08, 2)])
    if d == date(2026, 9, 15):
        sales[3][3] = ""; sales[9][3] = ""                       # two null item ids
        sales[5][4] = -1; sales[5][5] = -PRICE[sales[5][3]]       # negative quantity
        sales[12][3] = "I099"                                     # item that is not in the item master
        sales.append(list(sales[20]))                             # exact duplicate line
    write(f"pos_sales_{ymd}.csv", ["sale_id", "sale_line_no", "store_id", "item_id", "sale_qty", "sale_amt", "discount_amt", "tax_amt"], sales)
    # inventory snapshot (every item at every store; stock moves a little each day)
    inv = []
    for sid, _, _ in STORES:
        for iid, *_ in ITEMS:
            k = (iid, sid)
            stock[k] = stock.get(k, 40 + rng.randint(0, 40)) if d == DAYS[0] else max(0, stock[k] - rng.randint(0, 5) + (30 if rng.random() < 0.1 else 0))
            if d >= date(2026, 9, 16) and k in {("I001", "1001"), ("I002", "1002")}:
                continue                                          # position disappears
            inv.append([iid, sid, stock[k], 0 if stock[k] > 20 else 50, 15])
    write(f"inventory_snapshot_{ymd}.csv", ["item_id", "store_id", "on_hand_qty", "on_order_qty", "reorder_point_qty"], inv)
print("written to", os.path.abspath(OUT))
