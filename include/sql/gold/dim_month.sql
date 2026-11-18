INSERT INTO dim_month (month_id, month_no, month_nm, quarter_no, year_key)
SELECT '{{ month_id }}', {{ month_no }}, '{{ month_nm }}', {{ quarter_no }}, (SELECT MIN(year_key) FROM dim_year WHERE year_id = '{{ year_id }}')
WHERE NOT EXISTS (SELECT 1 FROM dim_month WHERE month_id = '{{ month_id }}');
