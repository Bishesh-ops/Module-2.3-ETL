-- Parents the source files do not provide: one UNKNOWN row each so the NOT NULL keys always resolve.
INSERT INTO dim_chain (chain_id, chain_nm, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown chain', {{ dt('1900-01-01') }} WHERE NOT EXISTS (SELECT 1 FROM dim_chain WHERE chain_id = 'UNKNOWN');
INSERT INTO dim_area (area_id, area_nm, chain_key, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown area', (SELECT MIN(chain_key) FROM dim_chain WHERE chain_id = 'UNKNOWN'), {{ dt('1900-01-01') }}
WHERE NOT EXISTS (SELECT 1 FROM dim_area WHERE area_id = 'UNKNOWN');
INSERT INTO dim_region (region_id, region_nm, area_key, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown region', (SELECT MIN(area_key) FROM dim_area WHERE area_id = 'UNKNOWN'), {{ dt('1900-01-01') }}
WHERE NOT EXISTS (SELECT 1 FROM dim_region WHERE region_id = 'UNKNOWN');
INSERT INTO dim_locn_type (locn_type_id, locn_type_nm, effective_start_dt)
SELECT 'STORE', 'Store', {{ dt('1900-01-01') }} WHERE NOT EXISTS (SELECT 1 FROM dim_locn_type WHERE locn_type_id = 'STORE');
INSERT INTO dim_department (department_id, department_nm, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown department', {{ dt('1900-01-01') }} WHERE NOT EXISTS (SELECT 1 FROM dim_department WHERE department_id = 'UNKNOWN');
INSERT INTO dim_category (category_id, category_nm, department_key, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown category', (SELECT MIN(department_key) FROM dim_department WHERE department_id = 'UNKNOWN'), {{ dt('1900-01-01') }}
WHERE NOT EXISTS (SELECT 1 FROM dim_category WHERE category_id = 'UNKNOWN');
INSERT INTO dim_brand (brand_id, brand_nm, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown brand', {{ dt('1900-01-01') }} WHERE NOT EXISTS (SELECT 1 FROM dim_brand WHERE brand_id = 'UNKNOWN');
INSERT INTO dim_supplier (supplier_id, supplier_nm, effective_start_dt)
SELECT 'UNKNOWN', 'Unknown supplier', {{ dt('1900-01-01') }} WHERE NOT EXISTS (SELECT 1 FROM dim_supplier WHERE supplier_id = 'UNKNOWN');
