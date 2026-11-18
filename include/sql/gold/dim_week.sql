INSERT INTO dim_week (week_id, week_no, week_start_dt, week_end_dt, year_key)
SELECT '{{ week_id }}', {{ week_no }}, {{ dt(week_start) }}, {{ dt(week_end) }}, (SELECT MIN(year_key) FROM dim_year WHERE year_id = '{{ year_id }}')
WHERE NOT EXISTS (SELECT 1 FROM dim_week WHERE week_id = '{{ week_id }}');
