INSERT INTO dim_year (year_id, year_no)
SELECT '{{ year_id }}', {{ year_no }} WHERE NOT EXISTS (SELECT 1 FROM dim_year WHERE year_id = '{{ year_id }}');
