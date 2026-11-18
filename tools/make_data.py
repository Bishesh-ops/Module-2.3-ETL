"""Expanded assignment dataset: 5 days (2026-09-13 .. 2026-09-17), 4 feeds per day, written to data/incoming.

~42 stores, 8 districts, 30 subclasses, ~430 everyday retail items, ~2,500 sales lines and ~5,500 inventory
positions per day. Deterministic: every random stream is seeded from a fixed text, nothing depends on hash order.
Standard library only.  (The original 12-item generator is kept as make_data_original.py.)

Planted scenarios (the original ones keep their ids so the old story still works):
  09-14  subclass SC2 renamed "Drinks" -> "Drinks & More"                       SCD1 (dim_subclass)
  09-14  item I020 gets a new name                                              SCD2 (dim_item)
  09-15  store 1004 moves district D2 -> D3                                     SCD2 (dim_locn)
  09-15  subclass SC10 renamed                                                  SCD1
  09-15  6 new items appear in the item master and start selling
  09-15  QUARANTINE day ("day 3"): sales  2 null item ids, 1 negative qty, 1 item not in the master (I9099), 1 duplicated line
                                    plus 1 non-numeric qty, 1 non-numeric amount, 1 unknown store, 1 line with the wrong field count
                                    master / inventory files also carry a duplicate, a blank subclass, null / orphan / negative rows
  09-16  item I007 moves subclass SC2 -> SC3                                    SCD2 (dim_item)
  09-16  store 1017 moves district D4 -> D6                                     SCD2
  09-16  district D5 renamed                                                    SCD1 (dim_district)
  09-16  stores 1041 and 1042 open (first appear in the store master)
  09-16  6 more new items; 4 items flagged DISCONTINUED (description change = SCD2); 40 inventory positions disappear
  09-17  the 4 discontinued items leave all stores' inventory; store 1040 closes; store 1022 moves D1 -> D8; item I050 changes subclass
"""
import bisect
import csv
import math
import os
import random
import sys
from datetime import date, timedelta

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "incoming")
DAYS = [date(2026, 9, 13) + timedelta(days=i) for i in range(5)]
D14, D15, D16, D17 = DAYS[1], DAYS[2], DAYS[3], DAYS[4]


def rng(*parts):
    return random.Random("|".join(str(p) for p in ("retail-v2",) + parts))     # text seeds are stable across runs


# ----------------------------------------------------------------------------------------------- catalogue
BRANDS = ["Sunfield", "Maple Lane", "Blue Harbor", "Goldleaf", "Nordic Pure", "Green Valley", "Everyday Basics",
          "Prime Choice", "Hearth & Home", "Bright Day", "Wild Orchard", "Summit", "Willow Creek", "Harvest Moon"]
DESCRIPTORS = ["everyday value range", "premium range", "family size", "great for sharing", "weekly essentials",
               "customer favourite", "new look packaging", "trusted quality"]
S = lambda *pairs: [(a, b) for a, b in pairs]                                       # noqa: E731
SUBS = [  # id, name, base price, uom, product types, sizes (label, price multiplier)
    ("SC1", "Snacks", 2.5, "EA", ["Potato Chips", "Tortilla Chips", "Pretzels", "Popcorn", "Trail Mix", "Rice Crackers"], S(("150g", 1), ("300g", 1.8), ("6-pack", 3.2))),
    ("SC2", "Drinks", 1.6, "EA", ["Cola", "Lemon Soda", "Orange Juice", "Apple Juice", "Iced Tea", "Ginger Ale"], S(("330ml", 1), ("1L", 1.9), ("6x330ml", 4.8))),
    ("SC3", "Household", 3.2, "EA", ["Dish Soap", "Laundry Detergent", "Paper Towels", "Trash Bags", "All-Purpose Cleaner", "Sponges"], S(("500ml", 1), ("1L", 1.7), ("2-pack", 1.9))),
    ("SC4", "Dairy", 1.9, "EA", ["Whole Milk", "Greek Yogurt", "Cheddar Cheese", "Salted Butter", "Cream Cheese", "Oat Milk"], S(("500g", 1), ("1L", 1.6), ("250g", 0.7))),
    ("SC5", "Bakery", 2.2, "EA", ["Sourdough Loaf", "Bagels", "Croissants", "Wholemeal Bread", "Blueberry Muffins", "Flour Tortillas"], S(("single", 1), ("4-pack", 2.6), ("6-pack", 3.5))),
    ("SC6", "Fresh Produce", 2.8, "KG", ["Bananas", "Red Apples", "Carrots", "Vine Tomatoes", "Baby Spinach", "Avocados", "Potatoes", "Onions"], S(("loose", 1), ("bag 1kg", 1.1), ("bag 2kg", 2.0))),
    ("SC7", "Meat & Poultry", 6.5, "KG", ["Chicken Breast", "Minced Beef", "Pork Chops", "Turkey Slices", "Smoked Bacon", "Pork Sausages"], S(("500g", 1), ("1kg", 1.9), ("family pack", 2.6))),
    ("SC8", "Seafood", 8.5, "EA", ["Salmon Fillet", "King Prawns", "Tuna Steak", "Cod Fillet", "Crab Sticks"], S(("200g", 1), ("400g", 1.8), ("1kg", 4.2))),
    ("SC9", "Frozen Foods", 3.8, "EA", ["Margherita Pizza", "Vanilla Ice Cream", "Garden Peas", "Fish Fingers", "Mixed Berries", "Waffles"], S(("400g", 1), ("750g", 1.6), ("1kg", 2.0))),
    ("SC10", "Breakfast Cereal", 3.4, "EA", ["Corn Flakes", "Crunchy Granola", "Oat Porridge", "Chocolate Rings", "Fruit Muesli"], S(("375g", 1), ("750g", 1.7), ("1kg", 2.1))),
    ("SC11", "Pasta & Rice", 2.0, "EA", ["Spaghetti", "Penne", "Basmati Rice", "Jasmine Rice", "Couscous", "Egg Noodles"], S(("500g", 1), ("1kg", 1.8), ("2kg", 3.3))),
    ("SC12", "Canned Goods", 1.3, "EA", ["Chopped Tomatoes", "Baked Beans", "Sweetcorn", "Chickpeas", "Tuna in Brine", "Coconut Milk"], S(("400g", 1), ("2x400g", 1.9), ("800g", 1.7))),
    ("SC13", "Sauces & Condiments", 2.4, "EA", ["Tomato Ketchup", "Mayonnaise", "Soy Sauce", "Hot Sauce", "Barbecue Sauce", "Yellow Mustard"], S(("250ml", 1), ("500ml", 1.7), ("squeezy 400g", 1.4))),
    ("SC14", "Coffee & Tea", 4.5, "EA", ["Ground Coffee", "Instant Coffee", "Green Tea", "Black Tea Bags", "Coffee Pods", "Chamomile Tea"], S(("100g", 1), ("250g", 2.1), ("40 bags", 1.4))),
    ("SC15", "Confectionery", 1.9, "EA", ["Milk Chocolate Bar", "Dark Chocolate Bar", "Gummy Bears", "Fruit Drops", "Chocolate Biscuits"], S(("100g", 1), ("200g", 1.8), ("multipack", 3.0))),
    ("SC16", "Baby Care", 6.5, "EA", ["Baby Wipes", "Nappies Size 3", "Infant Formula", "Baby Shampoo", "Fruit Puree Pouches"], S(("single", 1), ("value pack", 2.2), ("travel size", 0.6))),
    ("SC17", "Pet Supplies", 5.5, "EA", ["Dry Dog Food", "Cat Food Pouches", "Cat Litter", "Dog Chew Treats", "Rubber Chew Toy"], S(("1kg", 1), ("3kg", 2.5), ("12 pouches", 1.8))),
    ("SC18", "Personal Care", 3.6, "EA", ["Shampoo", "Conditioner", "Body Wash", "Fluoride Toothpaste", "Roll-On Deodorant", "Hand Soap"], S(("250ml", 1), ("500ml", 1.6), ("travel size", 0.5))),
    ("SC19", "Health & Wellness", 5.9, "EA", ["Vitamin C Tablets", "Multivitamin", "Pain Relief Tablets", "Hand Sanitiser", "Fabric Plasters"], S(("30 pack", 1), ("60 pack", 1.7), ("100ml", 0.8))),
    ("SC20", "Beauty", 7.2, "EA", ["Moisturising Face Cream", "Lip Balm", "Sun Cream SPF30", "Mascara", "Nail Polish"], S(("50ml", 1), ("100ml", 1.6), ("mini", 0.5))),
    ("SC21", "Electronics Accessories", 9.5, "EA", ["USB-C Cable", "Phone Charger", "Wired Earbuds", "Power Bank", "AA Batteries"], S(("1m", 1), ("2m", 1.4), ("4-pack", 1.2))),
    ("SC22", "Kitchenware", 11.0, "EA", ["Non-Stick Frying Pan", "Chef Knife", "Mixing Bowl Set", "Food Storage Containers", "Bamboo Chopping Board"], S(("small", 1), ("medium", 1.4), ("large", 1.9))),
    ("SC23", "Home Textiles", 12.0, "EA", ["Bath Towel", "Cotton Bed Sheet", "Memory Foam Pillow", "Fleece Throw Blanket", "Cushion Cover"], S(("single", 1), ("double", 1.5), ("2-pack", 1.8))),
    ("SC24", "Stationery", 2.6, "EA", ["Spiral Notebook", "Ballpoint Pens", "Highlighters", "A4 Printer Paper", "Sticky Notes"], S(("pack of 5", 1), ("pack of 10", 1.7), ("500 sheets", 2.4))),
    ("SC25", "Toys & Games", 8.5, "EA", ["Jigsaw Puzzle", "Playing Cards", "Building Blocks", "Family Board Game", "Colouring Set"], S(("small", 1), ("medium", 1.5), ("deluxe", 2.3))),
    ("SC26", "Garden", 5.2, "EA", ["Potting Soil", "Vegetable Seeds", "Garden Gloves", "Watering Can", "Terracotta Plant Pot"], S(("small", 1), ("medium", 1.5), ("large", 2.2))),
    ("SC27", "Sports & Outdoors", 10.5, "EA", ["Yoga Mat", "Steel Water Bottle", "Tennis Balls", "Skipping Rope", "Camping Torch"], S(("standard", 1), ("pro", 1.8), ("kids", 0.8))),
    ("SC28", "Bottled Water", 0.9, "EA", ["Still Spring Water", "Sparkling Water", "Lemon Flavoured Water", "Coconut Water"], S(("500ml", 1), ("1.5L", 1.5), ("6x1.5L", 5.5))),
    ("SC29", "Hardware", 4.8, "EA", ["LED Light Bulb", "Duct Tape", "Screwdriver Set", "Extension Lead", "Picture Hooks"], S(("standard", 1), ("2-pack", 1.8), ("value pack", 2.6))),
    ("SC30", "Automotive", 7.8, "EA", ["Motor Oil 5W-30", "Windscreen Wash", "Car Air Freshener", "Tyre Inflator", "Microfibre Cloths"], S(("small", 1), ("1L", 1.6), ("5L", 4.8))),
]
SUB_NAME = {s[0]: s[1] for s in SUBS}
SUB_IDS = [s[0] for s in SUBS]


def sub_name(sid, d):
    if sid == "SC2" and d >= D14:
        return "Drinks & More"
    if sid == "SC10" and d >= D15:
        return "Cereal & Breakfast"
    return SUB_NAME[sid]


def build_items():
    items, used = [], set()
    for sid, name, base, uom, types, sizes in SUBS:
        r = rng("items", sid)
        combos = [(t, s) for t in types for s in sizes]
        r.shuffle(combos)
        for t, (label, mult) in combos[: min(len(combos), 11 + r.randint(0, 4))]:
            brand = r.choice(BRANDS)
            while (brand, t, label) in used:
                brand = r.choice(BRANDS)
            used.add((brand, t, label))
            price = max(0.99, round(base * mult * r.uniform(0.8, 1.35) * 2) / 2 - 0.01)
            items.append({"sub": sid, "uom": uom if label not in ("multipack", "2-pack", "4-pack", "6-pack") else "PK", "nm": f"{brand} {t} {label}",
                          "desc": f"{brand} {t}, {label}, {r.choice(DESCRIPTORS)}", "price": round(price, 2), "pop": r.lognormvariate(0, 0.9),
                          "intro": None, "disc": None})
    # ids: the first four items of SC1..SC3 are I001..I012 (the original scenario ids), the rest follow in order
    core, rest, seen = [], [], {"SC1": 0, "SC2": 0, "SC3": 0}
    for it in items:
        if it["sub"] in seen and seen[it["sub"]] < 4:
            seen[it["sub"]] += 1
            core.append(it)
        else:
            rest.append(it)
    ordered = sorted(core, key=lambda i: SUB_IDS.index(i["sub"])) + rest
    for n, it in enumerate(ordered, start=1):
        it["id"] = f"I{n:03d}"
    return ordered


ITEMS = build_items()
BY_ID = {i["id"]: i for i in ITEMS}
_r = rng("lifecycle")
_pool = [i for i in ITEMS if int(i["id"][1:]) > 60]
_r.shuffle(_pool)
for it in _pool[:6]:
    it["intro"] = D15
for it in _pool[6:12]:
    it["intro"] = D16
for it in _pool[12:16]:
    it["disc"] = D16                                   # description change on 09-16, leaves the stores on 09-17
NEW_I050_SUB = SUB_IDS[(SUB_IDS.index(BY_ID["I050"]["sub"]) + 1) % len(SUB_IDS)]


def item_sub(it, d):
    if it["id"] == "I007" and d >= D16:
        return "SC3"
    if it["id"] == "I050" and d >= D17:
        return NEW_I050_SUB
    return it["sub"]


def item_active(it, d):
    return it["intro"] is None or d >= it["intro"]


def item_row(it, d):
    nm = it["nm"] + (" (New Recipe)" if it["id"] == "I020" and d >= D14 else "")
    desc = it["desc"] + (" - DISCONTINUED" if it["disc"] and d >= it["disc"] else "")
    sid = item_sub(it, d)
    return [it["id"], nm, desc, it["uom"], sid, sub_name(sid, d)]


# ------------------------------------------------------------------------------------------------ stores
PLACES = ["Riverside", "Lakeview", "Maple Grove", "Oak Park", "Harbor Point", "Cedar Hill", "Elm Street", "Fairview", "Greenfield",
          "Highland", "Ironbridge", "Jasper Court", "Kingsway", "Lindenwood", "Millbrook", "Northgate", "Orchard Square", "Pinecrest",
          "Queens Cross", "Redwood", "Stonebridge", "Thornfield", "Union Square", "Valley Mall", "Westbrook", "Yorkshire Plaza", "Ashford",
          "Brookside", "Clearwater", "Dunmore", "Eastgate", "Foxglove", "Glenwood", "Hawthorn", "Ivy Lane", "Juniper", "Kestrel Park"]
DISTRICT_NAMES = ["One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight"]


def build_stores():
    stores = []
    first = {1001: ("Store North", 1), 1002: ("Store South", 1), 1003: ("Store East", 2), 1004: ("Store West", 2), 1005: ("Store Central", 3)}
    for sid in range(1001, 1043):
        if sid in first:
            nm, dist = first[sid]
        else:
            nm, dist = f"{PLACES[(sid - 1006) % len(PLACES)]} Store", ((sid - 1006) % 8) + 1
        r = rng("store", sid)
        stores.append({"id": str(sid), "nm": nm, "dist": f"D{dist}", "traffic": r.lognormvariate(0, 0.35), "size": 95 + int(r.random() * 70)})
    return stores


STORES = build_stores()


def store_open(s, d):
    if s["id"] in ("1041", "1042"):
        return d >= D16
    if s["id"] == "1040":
        return d < D17
    return True


def store_dist(s, d):
    if s["id"] == "1004" and d >= D15:
        return "D3"
    if s["id"] == "1017" and d >= D16:
        return "D6"
    if s["id"] == "1022" and d >= D17:
        return "D8"
    return s["dist"]


def dist_name(did, d):
    n = DISTRICT_NAMES[int(did[1:]) - 1]
    return f"District {n}" + (" East" if did == "D5" and d >= D16 else "")


def store_row(s, d):
    dist = store_dist(s, d)
    return [s["id"], s["nm"], dist, dist_name(dist, d), "USD"]


# ------------------------------------------------------------------------------------------ simulation
PROMOS = {"SC1": (DAYS[0], DAYS[4], 0.15), "SC9": (D14, D17, 0.20), "SC15": (D15, D17, 0.25), "SC14": (DAYS[0], D15, 0.10), "SC18": (D16, D17, 0.20)}
DAY_FACTOR = {0: 1.0, 1: 0.95, 2: 1.0, 3: 1.02, 4: 1.15, 5: 1.35, 6: 1.2}          # Monday = 0
DISRUPTED = {i["id"] for i in sorted(ITEMS, key=lambda i: -i["pop"])[:14] if int(i["id"][1:]) > 12}   # supplier delays from 09-14


def fmt(x):
    return f"{x:.2f}"


def new_position(r, it):
    rp = max(6, int(it["pop"] * 8) + r.randint(4, 14))
    onhand = r.randint(0, 3) if r.random() < 0.03 else int(rp * r.uniform(1.2, 5))
    return {"onhand": onhand, "onorder": 0, "arrive": None, "rp": rp}


def simulate():
    """Returns {date: {"sales": [...], "inv": [...]}}; stock moves with the sales so the files agree with each other."""
    pos = {}                                    # (store id, item id) -> position
    assort = {}                                 # store id -> set of item ids
    out = {}
    base_items = [i for i in ITEMS if i["intro"] is None]
    core_ids = [i["id"] for i in ITEMS[:12]]
    removed_d16 = None
    for d in DAYS:
        r = rng("day", d.isoformat())
        # --- structure: new stores, new items, delistings, closures
        for s in STORES:
            if not store_open(s, d):
                for iid in list(assort.get(s["id"], ())):
                    assort[s["id"]].discard(iid); pos.pop((s["id"], iid), None)
                continue
            if s["id"] not in assort:
                ra = rng("assort", s["id"])
                cand = [i["id"] for i in ITEMS if item_active(i, d) and i["id"] not in core_ids and not (i["disc"] and d >= D17)]
                ra.shuffle(cand)
                assort[s["id"]] = set(core_ids) | set(cand[: s["size"]])
                for iid in assort[s["id"]]:
                    pos[(s["id"], iid)] = new_position(rng("pos", s["id"], iid), BY_ID[iid])
        for it in ITEMS:
            if it["intro"] == d:                                     # new item reaches ~60% of the stores
                for s in STORES:
                    if store_open(s, d) and s["id"] in assort and rng("intro", it["id"], s["id"]).random() < 0.6:
                        assort[s["id"]].add(it["id"]); pos[(s["id"], it["id"])] = new_position(rng("pos", s["id"], it["id"]), it)
        if d == D16:                                                 # 40 positions delisted, including I001@1001 and I002@1002
            rr = rng("delist")
            pick = sorted((s, i) for (s, i) in pos if i not in core_ids and s not in ("1041", "1042"))
            rr.shuffle(pick)
            for key in [("1001", "I001"), ("1002", "I002")] + pick[:38]:
                if key in pos:
                    pos.pop(key); assort[key[0]].discard(key[1])
        if d == D17:                                                 # discontinued items leave every store
            for it in ITEMS:
                if it["disc"]:
                    for s in STORES:
                        pos.pop((s["id"], it["id"]), None)
                        if s["id"] in assort:
                            assort[s["id"]].discard(it["id"])
        # --- deliveries arrive
        for p in pos.values():
            if p["onorder"] and p["arrive"] <= d:
                p["onhand"] += p["onorder"]; p["onorder"] = 0; p["arrive"] = None
        # --- sales
        sales = []
        for s in STORES:
            if not store_open(s, d) or s["id"] not in assort:
                continue
            items = sorted(assort[s["id"]])
            weights, acc = [], 0.0
            for iid in items:
                it = BY_ID[iid]
                promo = PROMOS.get(item_sub(it, d))
                boost = 1.6 if promo and promo[0] <= d <= promo[1] else 1.0
                acc += it["pop"] * boost
                weights.append(acc)
            n_tx = max(8, round(32 * s["traffic"] * DAY_FACTOR[d.weekday()] * r.uniform(0.9, 1.1)))
            for t in range(1, n_tx + 1):
                k = 1 + (r.random() < 0.55) + (r.random() < 0.25) + (r.random() < 0.1)
                line = 0
                for _ in range(k):
                    iid = items[min(bisect.bisect_left(weights, r.random() * acc), len(items) - 1)]
                    p, it = pos[(s["id"], iid)], BY_ID[iid]
                    u = r.random()
                    qty = 1 if u < 0.7 else 2 if u < 0.9 else 3 if u < 0.97 else r.randint(4, 6)
                    qty = min(qty, p["onhand"])
                    if qty <= 0:
                        continue                                      # out of stock: no sale
                    p["onhand"] -= qty
                    line += 1
                    gross = qty * it["price"]
                    promo = PROMOS.get(item_sub(it, d))
                    disc = round(gross * promo[2], 2) if promo and promo[0] <= d <= promo[1] else 0.0
                    net = round(gross - disc, 2)
                    sales.append([f"S{d:%m%d}{s['id']}{t:04d}", line, s["id"], iid, qty, fmt(net), fmt(disc), fmt(net * 0.08)])
        # --- replenishment decisions at close of business, then the end-of-day snapshot
        inv = []
        for (sid, iid) in sorted(pos):
            p = pos[(sid, iid)]
            if not p["onorder"] and p["onhand"] <= p["rp"]:
                p["onorder"] = p["rp"] * 3
                p["arrive"] = d + timedelta(days=2 + (3 if iid in DISRUPTED and d >= D14 else 0))
            inv.append([iid, sid, p["onhand"], p["onorder"], p["rp"]])
        out[d] = {"sales": sales, "inv": inv}
    return out


# ------------------------------------------------------------------------------------------ writing
def write(name, header, rows):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(header); w.writerows(rows)


def main():
    sim = simulate()
    for d in DAYS:
        ymd = d.strftime("%Y%m%d")
        items = [item_row(i, d) for i in ITEMS if item_active(i, d)]
        stores = [store_row(s, d) for s in STORES if store_open(s, d)]
        sales, inv = sim[d]["sales"], sim[d]["inv"]
        shape_row = None
        if d == D15:                                                 # ---- the quarantine day
            r = rng("defects")
            idx = r.sample(range(len(sales)), 8)
            sales[idx[0]][3] = ""; sales[idx[1]][3] = ""                                   # two null item ids
            sales[idx[2]][4] = -1; sales[idx[2]][5] = fmt(-float(sales[idx[2]][5]))   # negative quantity (and amount)
            sales[idx[3]][3] = "I9099"                                                      # item missing from the master
            sales[idx[4]][4] = "abc"                                                       # non-numeric quantity
            sales[idx[5]][5] = "n/a"                                                       # non-numeric amount
            sales[idx[6]][2] = "9999"                                                      # store not in the store master
            sales.append(list(sales[idx[7]]))                                              # exact duplicate line
            shape_row = sales[idx[7]][:4] + ["1", "9.99", "0.00", "0.80", "EXTRA"]         # wrong number of fields
            items.append(list(items[10]))                                                  # duplicate master row
            items.append(["I9990", "Test item without subclass", "", "EA", "", ""])        # blank subclass
            stores.append(list(stores[9]))                                                 # duplicate master row
            taken = {(row[1], row[0]) for row in inv}
            spare = next(i["id"] for i in ITEMS if i["intro"] is None and i["id"] not in [x[0] for x in items[:12]] and ("1003", i["id"]) not in taken)
            inv.append(list(inv[40]))                                                      # duplicate position
            inv.append(["", "1001", 10, 0, 15])                                            # null item id
            inv.append(["I9998", "1002", 12, 0, 15])                                       # item not in the master
            inv.append([spare, "1003", -5, 0, 15])                                         # negative stock on a position that was never stocked
        write(f"item_master_{ymd}.csv", ["item_id", "item_nm", "item_desc", "uom_cd", "subclass_id", "subclass_nm"], items)
        write(f"store_master_{ymd}.csv", ["store_id", "store_nm", "district_id", "district_nm", "currency_cd"], stores)
        write(f"pos_sales_{ymd}.csv", ["sale_id", "sale_line_no", "store_id", "item_id", "sale_qty", "sale_amt", "discount_amt", "tax_amt"],
              sales + ([shape_row] if shape_row else []))
        write(f"inventory_snapshot_{ymd}.csv", ["item_id", "store_id", "on_hand_qty", "on_order_qty", "reorder_point_qty"], inv)
        print(f"{d}: items {len(items)}  stores {len(stores)}  sales {len(sales)}  inventory {len(inv)}")
    print("written to", os.path.abspath(OUT))


if __name__ == "__main__":
    main()
