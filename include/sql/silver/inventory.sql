-- Silver inventory: clean the snapshot and compare each position with the Gold row that covers the run date.
-- action: NEW | NOCHANGE | CHANGE (close the row, open a new one) | CORRECT (same-day fix) | STALE (old day re-run, ignore)
DELETE FROM silver_inventory WHERE run_date = {{ dt(ds) }};

INSERT INTO silver_inventory (run_date, item_id, store_id, on_hand_qty, on_order_qty, reorder_point_qty, src_row_no, reject_cd, action_cd)
SELECT {{ dt(ds) }}, x.item_id, x.store_id, x.on_hand, x.on_order, x.reorder, x.src_row_no, x.reject_cd,
  CASE WHEN x.reject_cd IS NOT NULL THEN NULL
       WHEN o.item_key IS NULL THEN
            CASE WHEN EXISTS (SELECT 1 FROM fct_inventory e JOIN dim_item ei ON ei.item_key = e.item_key JOIN dim_locn el ON el.locn_key = e.locn_key
                              WHERE ei.item_id = x.item_id AND el.locn_id = x.store_id AND e.effective_start_dt > {{ dt(ds) }}) THEN 'STALE' ELSE 'NEW' END
       WHEN o.on_hand_qty = x.on_hand AND o.on_order_qty = x.on_order AND o.reorder_point_qty = x.reorder THEN 'NOCHANGE'
       WHEN o.effective_start_dt = {{ dt(ds) }} THEN 'CORRECT'
       WHEN o.effective_end_dt = {{ dt('9999-12-31') }} THEN 'CHANGE'
       ELSE 'STALE' END
FROM (
  SELECT b.*, CASE WHEN b.item_id = '' OR b.store_id = '' THEN 'NULL_KEY'
                   WHEN b.on_hand IS NULL OR b.on_order IS NULL OR b.reorder IS NULL THEN 'BAD_QTY'
                   WHEN b.on_hand < 0 THEN 'NEGATIVE_QTY'
                   WHEN b.rn > 1 THEN 'DUPLICATE'
                   WHEN NOT EXISTS (SELECT 1 FROM silver_item i WHERE i.run_date = {{ dt(ds) }} AND i.item_id = b.item_id AND i.reject_cd IS NULL) THEN 'ORPHAN_ITEM'
                   WHEN NOT EXISTS (SELECT 1 FROM silver_store s WHERE s.run_date = {{ dt(ds) }} AND s.store_id = b.store_id AND s.reject_cd IS NULL) THEN 'ORPHAN_STORE'
              END AS reject_cd
  FROM (
    SELECT COALESCE(TRIM(item_id), '') AS item_id, COALESCE(TRIM(store_id), '') AS store_id,
           {{ n('on_hand_qty') }} AS on_hand, {{ n0('on_order_qty') }} AS on_order, {{ n0('reorder_point_qty') }} AS reorder, src_row_no,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(TRIM(item_id), ''), COALESCE(TRIM(store_id), '') ORDER BY src_row_no) AS rn
    FROM stg_inventory WHERE batch_id = '{{ ds }}'
  ) b
) x
LEFT JOIN (
  SELECT f.*, i.item_id AS item_bk, l.locn_id AS locn_bk
  FROM fct_inventory f JOIN dim_item i ON i.item_key = f.item_key JOIN dim_locn l ON l.locn_key = f.locn_key
) o ON o.item_bk = x.item_id AND o.locn_bk = x.store_id AND o.effective_start_dt <= {{ dt(ds) }} AND o.effective_end_dt >= {{ dt(ds) }};
