-- fct_sales: delete the day, then insert it again. Each line gets the item and store version valid on the sale date.
DELETE FROM fct_sales WHERE day_key = (SELECT day_key FROM dim_day WHERE day_dt = {{ dt(ds) }});

INSERT INTO fct_sales (sale_id, sale_line_no, item_key, locn_key, day_key, customer_key, sale_qty, sale_amt, discount_amt, tax_amt)
SELECT s.sale_id, s.sale_line_no, i.item_key, l.locn_key, d.day_key, NULL, s.sale_qty, s.sale_amt, s.discount_amt, s.tax_amt
FROM silver_sales s
JOIN dim_day d ON d.day_dt = {{ dt(ds) }}
JOIN dim_item i ON i.item_id = s.item_id AND i.effective_start_dt <= {{ dt(ds) }} AND i.effective_end_dt >= {{ dt(ds) }}
JOIN dim_locn l ON l.locn_id = s.store_id AND l.effective_start_dt <= {{ dt(ds) }} AND l.effective_end_dt >= {{ dt(ds) }}
WHERE s.run_date = {{ dt(ds) }} AND s.reject_cd IS NULL;
