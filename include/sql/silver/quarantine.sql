-- Quarantine: copy every rejected row of the run date into rej_row with its reason. Rebuilt per run date.
DELETE FROM rej_row WHERE run_date = {{ dt(ds) }} AND reject_cd <> 'BAD_ROW_SHAPE';

INSERT INTO rej_row (run_date, source, reject_cd, src_key, amount)
SELECT run_date, 'item_master', reject_cd, 'row ' || CAST(src_row_no AS varchar) || ': item ' || COALESCE(item_id, ''), NULL
FROM silver_item WHERE run_date = {{ dt(ds) }} AND reject_cd IS NOT NULL;

INSERT INTO rej_row (run_date, source, reject_cd, src_key, amount)
SELECT run_date, 'store_master', reject_cd, 'row ' || CAST(src_row_no AS varchar) || ': store ' || COALESCE(store_id, ''), NULL
FROM silver_store WHERE run_date = {{ dt(ds) }} AND reject_cd IS NOT NULL;

INSERT INTO rej_row (run_date, source, reject_cd, src_key, amount)
SELECT run_date, 'pos_sales', reject_cd, 'row ' || CAST(src_row_no AS varchar) || ': sale ' || sale_id || '/' || COALESCE(CAST(sale_line_no AS varchar), '') || ' item ' || COALESCE(item_id, ''), sale_amt
FROM silver_sales WHERE run_date = {{ dt(ds) }} AND reject_cd IS NOT NULL;

INSERT INTO rej_row (run_date, source, reject_cd, src_key, amount)
SELECT run_date, 'inventory_snapshot', reject_cd, 'row ' || CAST(src_row_no AS varchar) || ': item ' || COALESCE(item_id, '') || ' store ' || COALESCE(store_id, ''), NULL
FROM silver_inventory WHERE run_date = {{ dt(ds) }} AND reject_cd IS NOT NULL;
