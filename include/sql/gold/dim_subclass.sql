-- dim_subclass is SCD1: a changed name is overwritten in place, no history.
-- effective_start_dt holds the date the current value was applied; an older day re-run cannot overwrite a newer value.
UPDATE dim_subclass SET subclass_nm = s.subclass_nm, effective_start_dt = {{ dt(ds) }}, update_ts = CURRENT_TIMESTAMP
FROM (SELECT subclass_id, MAX(subclass_nm) AS subclass_nm FROM silver_item
      WHERE run_date = {{ dt(ds) }} AND reject_cd IS NULL GROUP BY subclass_id) s
WHERE dim_subclass.subclass_id = s.subclass_id AND dim_subclass.subclass_nm <> s.subclass_nm
  AND dim_subclass.effective_start_dt <= {{ dt(ds) }};

INSERT INTO dim_subclass (subclass_id, subclass_nm, category_key, effective_start_dt)
SELECT s.subclass_id, s.subclass_nm, (SELECT MIN(category_key) FROM dim_category WHERE category_id = 'UNKNOWN'), {{ dt('1900-01-01') }}
FROM (SELECT subclass_id, MAX(subclass_nm) AS subclass_nm FROM silver_item
      WHERE run_date = {{ dt(ds) }} AND reject_cd IS NULL GROUP BY subclass_id) s
WHERE NOT EXISTS (SELECT 1 FROM dim_subclass d WHERE d.subclass_id = s.subclass_id);
