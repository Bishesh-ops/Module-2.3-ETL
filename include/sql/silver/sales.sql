-- Silver sales: every source line gets either NULL (good) or a reject code. First matching rule wins.
DELETE FROM silver_sales WHERE run_date = {{ dt(ds) }};

INSERT INTO silver_sales (run_date, sale_id, sale_line_no, store_id, item_id, sale_qty, sale_amt, discount_amt, tax_amt, src_row_no, reject_cd)
SELECT {{ dt(ds) }}, b.sale_id, b.line_no, b.store_id, b.item_id, b.qty, b.amt, b.disc, b.tax, b.src_row_no,
  CASE WHEN b.sale_id = '' OR b.line_no IS NULL THEN 'NULL_KEY'
       WHEN b.item_id = '' THEN 'NULL_ITEM'
       WHEN b.qty IS NULL THEN 'BAD_QTY'
       WHEN b.amt IS NULL OR b.disc IS NULL OR b.tax IS NULL THEN 'BAD_AMOUNT'
       WHEN b.qty <= 0 THEN 'NEGATIVE_QTY'
       WHEN b.rn > 1 THEN 'DUPLICATE'
       WHEN NOT EXISTS (SELECT 1 FROM silver_item i WHERE i.run_date = {{ dt(ds) }} AND i.item_id = b.item_id AND i.reject_cd IS NULL) THEN 'ORPHAN_ITEM'
       WHEN NOT EXISTS (SELECT 1 FROM silver_store s WHERE s.run_date = {{ dt(ds) }} AND s.store_id = b.store_id AND s.reject_cd IS NULL) THEN 'ORPHAN_STORE'
  END
FROM (
  SELECT COALESCE(TRIM(sale_id), '') AS sale_id, {{ n('sale_line_no') }} AS line_no, COALESCE(TRIM(store_id), '') AS store_id,
         COALESCE(TRIM(item_id), '') AS item_id, {{ n('sale_qty') }} AS qty, {{ n('sale_amt') }} AS amt,
         {{ n0('discount_amt') }} AS disc, {{ n0('tax_amt') }} AS tax, src_row_no,
         ROW_NUMBER() OVER (PARTITION BY COALESCE(TRIM(sale_id), ''), COALESCE(TRIM(sale_line_no), '') ORDER BY src_row_no) AS rn
  FROM stg_pos_sales WHERE batch_id = '{{ ds }}'
) b;
