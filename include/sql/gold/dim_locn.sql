-- dim_locn is SCD2: a store moving to another district closes its version and opens a new one.
UPDATE dim_locn SET locn_nm = s.store_nm, district_key = d.district_key, currency_cd = s.currency_cd, update_ts = CURRENT_TIMESTAMP
FROM silver_store s JOIN dim_district d ON d.district_id = s.district_id
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd = 'CORRECT' AND dim_locn.locn_id = s.store_id AND dim_locn.effective_start_dt = {{ dt(ds) }};

UPDATE dim_locn SET effective_end_dt = {{ dadd(dt(ds), -1) }}, active_flg = FALSE, update_ts = CURRENT_TIMESTAMP
FROM silver_store s
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd = 'CHANGE' AND dim_locn.locn_id = s.store_id AND dim_locn.effective_end_dt = {{ dt('9999-12-31') }};

INSERT INTO dim_locn (locn_id, locn_nm, district_key, locn_type_key, currency_cd, effective_start_dt, effective_end_dt)
SELECT s.store_id, s.store_nm, d.district_key, (SELECT MIN(locn_type_key) FROM dim_locn_type WHERE locn_type_id = 'STORE'), s.currency_cd,
       CASE WHEN s.action_cd = 'NEW' THEN {{ dt('1900-01-01') }} ELSE {{ dt(ds) }} END, {{ dt('9999-12-31') }}
FROM silver_store s JOIN dim_district d ON d.district_id = s.district_id
WHERE s.run_date = {{ dt(ds) }} AND s.action_cd IN ('NEW', 'CHANGE');
