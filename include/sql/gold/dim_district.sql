-- dim_district is SCD1 (same rule as dim_subclass).
UPDATE dim_district SET district_nm = s.district_nm, effective_start_dt = {{ dt(ds) }}, update_ts = CURRENT_TIMESTAMP
FROM (SELECT district_id, MAX(district_nm) AS district_nm FROM silver_store
      WHERE run_date = {{ dt(ds) }} AND reject_cd IS NULL GROUP BY district_id) s
WHERE dim_district.district_id = s.district_id AND dim_district.district_nm <> s.district_nm
  AND dim_district.effective_start_dt <= {{ dt(ds) }};

INSERT INTO dim_district (district_id, district_nm, region_key, effective_start_dt)
SELECT s.district_id, s.district_nm, (SELECT MIN(region_key) FROM dim_region WHERE region_id = 'UNKNOWN'), {{ dt('1900-01-01') }}
FROM (SELECT district_id, MAX(district_nm) AS district_nm FROM silver_store
      WHERE run_date = {{ dt(ds) }} AND reject_cd IS NULL GROUP BY district_id) s
WHERE NOT EXISTS (SELECT 1 FROM dim_district d WHERE d.district_id = s.district_id);
