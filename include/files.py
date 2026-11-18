FILES = [
    {"key": "item_master", "pattern": "item_master_{ymd}.csv", "table": "stg_item_master",
     "columns": ["item_id", "item_nm", "item_desc", "uom_cd", "subclass_id", "subclass_nm"]},
    {"key": "store_master", "pattern": "store_master_{ymd}.csv", "table": "stg_store_master",
     "columns": ["store_id", "store_nm", "district_id", "district_nm", "currency_cd"]},
    {"key": "pos_sales", "pattern": "pos_sales_{ymd}.csv", "table": "stg_pos_sales",
     "columns": ["sale_id", "sale_line_no", "store_id", "item_id", "sale_qty", "sale_amt", "discount_amt", "tax_amt"]},
    {"key": "inventory_snapshot", "pattern": "inventory_snapshot_{ymd}.csv", "table": "stg_inventory",
     "columns": ["item_id", "store_id", "on_hand_qty", "on_order_qty", "reorder_point_qty"]},
]
BY_KEY = {f["key"]: f for f in FILES}
KEYS = [f["key"] for f in FILES]


def file_name(key, ds):
    return BY_KEY[key]["pattern"].format(ymd=str(ds).replace("-", ""))
