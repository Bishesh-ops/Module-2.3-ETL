-- Reconciliation. Every row is (check, expected, actual); any difference fails the DAG.
SELECT 'file rows = staged + bad-shape rows' AS check_cd,
  (SELECT COALESCE(SUM(value_num), 0) FROM etl_run_log WHERE run_date = {{ dt(ds) }} AND metric = 'file_rows') AS expected,
  (SELECT COUNT(*) FROM stg_item_master WHERE batch_id = '{{ ds }}') + (SELECT COUNT(*) FROM stg_store_master WHERE batch_id = '{{ ds }}')
  + (SELECT COUNT(*) FROM stg_pos_sales WHERE batch_id = '{{ ds }}') + (SELECT COUNT(*) FROM stg_inventory WHERE batch_id = '{{ ds }}')
  + (SELECT COUNT(*) FROM rej_row WHERE run_date = {{ dt(ds) }} AND reject_cd = 'BAD_ROW_SHAPE') AS actual
UNION ALL
SELECT 'sales lines in file = gold + quarantined',
  (SELECT COUNT(*) FROM stg_pos_sales WHERE batch_id = '{{ ds }}'),
  (SELECT COUNT(*) FROM fct_sales f JOIN dim_day d ON d.day_key = f.day_key WHERE d.day_dt = {{ dt(ds) }})
  + (SELECT COUNT(*) FROM rej_row WHERE run_date = {{ dt(ds) }} AND source = 'pos_sales' AND reject_cd <> 'BAD_ROW_SHAPE')   -- bad-shape lines were never staged
UNION ALL
SELECT 'net sales in file = gold + quarantined',
  (SELECT COALESCE(SUM({{ n0('sale_amt') }}), 0) FROM stg_pos_sales WHERE batch_id = '{{ ds }}'),
  (SELECT COALESCE(SUM(f.sale_amt), 0) FROM fct_sales f JOIN dim_day d ON d.day_key = f.day_key WHERE d.day_dt = {{ dt(ds) }})
  + (SELECT COALESCE(SUM(amount), 0) FROM rej_row WHERE run_date = {{ dt(ds) }} AND source = 'pos_sales')
UNION ALL
SELECT 'inventory positions not matching gold (must be 0)', 0,
  (SELECT COUNT(*) FROM silver_inventory s
   WHERE s.run_date = {{ dt(ds) }} AND s.reject_cd IS NULL
     AND NOT EXISTS (SELECT 1 FROM fct_inventory f JOIN dim_item i ON i.item_key = f.item_key JOIN dim_locn l ON l.locn_key = f.locn_key
                     WHERE i.item_id = s.item_id AND l.locn_id = s.store_id AND f.on_hand_qty = s.on_hand_qty
                       AND f.effective_start_dt <= {{ dt(ds) }} AND f.effective_end_dt >= {{ dt(ds) }}))
UNION ALL
SELECT 'open inventory rows = positions in snapshot', 
  (SELECT COUNT(*) FROM silver_inventory WHERE run_date = {{ dt(ds) }} AND reject_cd IS NULL),
  (SELECT COUNT(*) FROM fct_inventory WHERE effective_start_dt <= {{ dt(ds) }} AND effective_end_dt >= {{ dt(ds) }});