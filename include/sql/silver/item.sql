-- Silver item: clean + dedupe + check the item master of the run date, and decide what Gold must do (action_cd).
-- action: NEW (never seen) | CHANGE (a tracked attribute differs -> new SCD2 version) | CORRECT (same-day fix, update in place)
--         NOCHANGE | STALE (a later version already exists: an old day is being re-run, ignore)
DELETE FROM silver_item WHERE run_date = {{ dt(ds) }};

INSERT INTO silver_item (run_date, item_id, item_nm, item_desc, uom_cd, subclass_id, subclass_nm, src_row_no, reject_cd, action_cd)
SELECT {{ dt(ds) }}, x.item_id, x.item_nm, x.item_desc, x.uom_cd, x.subclass_id, x.subclass_nm, x.src_row_no, x.reject_cd,
  CASE WHEN x.reject_cd IS NOT NULL THEN NULL
       WHEN o.item_key IS NULL THEN CASE WHEN EXISTS (SELECT 1 FROM dim_item e WHERE e.item_id = x.item_id) THEN 'STALE' ELSE 'NEW' END
       WHEN o.item_nm = x.item_nm AND o.item_desc = x.item_desc AND o.unit_of_measure_cd = x.uom_cd AND sc.subclass_id = x.subclass_id THEN 'NOCHANGE'
       WHEN o.effective_start_dt = {{ dt(ds) }} THEN 'CORRECT'
       WHEN o.effective_end_dt = {{ dt('9999-12-31') }} THEN 'CHANGE'
       ELSE 'STALE' END
FROM (
  SELECT b.*, CASE WHEN b.item_id = '' THEN 'NULL_KEY'
                   WHEN b.rn > 1 THEN 'DUPLICATE'
                   WHEN b.subclass_id = '' OR b.item_nm = '' THEN 'NULL_ATTRIBUTE' END AS reject_cd
  FROM (
    SELECT COALESCE(TRIM(item_id), '') AS item_id, COALESCE(TRIM(item_nm), '') AS item_nm, COALESCE(TRIM(item_desc), '') AS item_desc,
           COALESCE(TRIM(uom_cd), '') AS uom_cd, COALESCE(TRIM(subclass_id), '') AS subclass_id, COALESCE(TRIM(subclass_nm), '') AS subclass_nm,
           src_row_no,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(TRIM(item_id), '') ORDER BY src_row_no) AS rn
    FROM stg_item_master WHERE batch_id = '{{ ds }}'
  ) b
) x
LEFT JOIN dim_item o ON o.item_id = x.item_id AND o.effective_start_dt <= {{ dt(ds) }} AND o.effective_end_dt >= {{ dt(ds) }}
LEFT JOIN dim_subclass sc ON sc.subclass_key = o.subclass_key;
