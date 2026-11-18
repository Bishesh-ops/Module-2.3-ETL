-- fct_inventory is positional: one row per item/store per period of unchanged stock. Rows are closed, never deleted.
-- 1. same-day correction: update the row that started today
UPDATE fct_inventory SET on_hand_qty = s.on_hand_qty, on_order_qty = s.on_order_qty, reorder_point_qty = s.reorder_point_qty, update_ts = CURRENT_TIMESTAMP
FROM silver_inventory s, dim_item i, dim_locn l
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd = 'CORRECT' AND i.item_id = s.item_id AND l.locn_id = s.store_id
  AND fct_inventory.item_key = i.item_key AND fct_inventory.locn_key = l.locn_key AND fct_inventory.effective_start_dt = {{ dt(ds) }};

-- 2. stock changed: close the open row the day before
UPDATE fct_inventory SET effective_end_dt = {{ dadd(dt(ds), -1) }}, update_ts = CURRENT_TIMESTAMP
FROM silver_inventory s, dim_item i, dim_locn l
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd = 'CHANGE' AND i.item_id = s.item_id AND l.locn_id = s.store_id
  AND fct_inventory.item_key = i.item_key AND fct_inventory.locn_key = l.locn_key
  AND fct_inventory.effective_end_dt = {{ dt('9999-12-31') }} AND fct_inventory.effective_start_dt < {{ dt(ds) }};

-- 3. position missing from today's snapshot: close it (only when a snapshot arrived at all)
UPDATE fct_inventory SET effective_end_dt = {{ dadd(dt(ds), -1) }}, update_ts = CURRENT_TIMESTAMP
WHERE effective_end_dt = {{ dt('9999-12-31') }} AND effective_start_dt < {{ dt(ds) }}
  AND EXISTS (SELECT 1 FROM silver_inventory a WHERE a.run_date = {{ dt(ds) }})
  AND NOT EXISTS (SELECT 1 FROM silver_inventory s, dim_item i, dim_locn l
                  WHERE s.run_date = {{ dt(ds) }} AND i.item_id = s.item_id AND l.locn_id = s.store_id
                    AND i.item_key = fct_inventory.item_key AND l.locn_key = fct_inventory.locn_key);

-- 4. open new rows
INSERT INTO fct_inventory (item_key, locn_key, day_key, on_hand_qty, on_order_qty, reorder_point_qty, effective_start_dt, effective_end_dt)
SELECT i.item_key, l.locn_key, d.day_key, s.on_hand_qty, s.on_order_qty, s.reorder_point_qty, {{ dt(ds) }}, {{ dt('9999-12-31') }}
FROM silver_inventory s
JOIN dim_day d ON d.day_dt = {{ dt(ds) }}
JOIN dim_item i ON i.item_id = s.item_id AND i.effective_start_dt <= {{ dt(ds) }} AND i.effective_end_dt >= {{ dt(ds) }}
JOIN dim_locn l ON l.locn_id = s.store_id AND l.effective_start_dt <= {{ dt(ds) }} AND l.effective_end_dt >= {{ dt(ds) }}
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd IN ('NEW', 'CHANGE');
