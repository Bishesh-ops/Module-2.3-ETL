-- Silver store: same pattern as item (SCD2 attributes: name, district, currency).
DELETE FROM silver_store WHERE run_date = {{ dt(ds) }};

INSERT INTO silver_store (run_date, store_id, store_nm, district_id, district_nm, currency_cd, src_row_no, reject_cd, action_cd)
SELECT {{ dt(ds) }}, x.store_id, x.store_nm, x.district_id, x.district_nm, x.currency_cd, x.src_row_no, x.reject_cd,
  CASE WHEN x.reject_cd IS NOT NULL THEN NULL
       WHEN o.locn_key IS NULL THEN CASE WHEN EXISTS (SELECT 1 FROM dim_locn e WHERE e.locn_id = x.store_id) THEN 'STALE' ELSE 'NEW' END
       WHEN o.locn_nm = x.store_nm AND dd.district_id = x.district_id AND o.currency_cd = x.currency_cd THEN 'NOCHANGE'
       WHEN o.effective_start_dt = {{ dt(ds) }} THEN 'CORRECT'
       WHEN o.effective_end_dt = {{ dt('9999-12-31') }} THEN 'CHANGE'
       ELSE 'STALE' END
FROM (
  SELECT b.*, CASE WHEN b.store_id = '' THEN 'NULL_KEY'
                   WHEN b.rn > 1 THEN 'DUPLICATE'
                   WHEN b.district_id = '' OR b.store_nm = '' OR b.currency_cd = '' THEN 'NULL_ATTRIBUTE' END AS reject_cd
  FROM (
    SELECT COALESCE(TRIM(store_id), '') AS store_id, COALESCE(TRIM(store_nm), '') AS store_nm, COALESCE(TRIM(district_id), '') AS district_id,
           COALESCE(TRIM(district_nm), '') AS district_nm, COALESCE(TRIM(currency_cd), '') AS currency_cd, src_row_no,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(TRIM(store_id), '') ORDER BY src_row_no) AS rn
    FROM stg_store_master WHERE batch_id = '{{ ds }}'
  ) b
) x
LEFT JOIN dim_locn o ON o.locn_id = x.store_id AND o.effective_start_dt <= {{ dt(ds) }} AND o.effective_end_dt >= {{ dt(ds) }}
LEFT JOIN dim_district dd ON dd.district_key = o.district_key;
