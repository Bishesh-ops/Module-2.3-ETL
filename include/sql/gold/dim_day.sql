INSERT INTO dim_day (day_id, day_dt, weekday_nm, weekend_flg, week_key, month_key)
SELECT '{{ day_id }}', {{ dt(ds) }}, '{{ weekday_nm }}', {{ weekend }},
       (SELECT MIN(week_key) FROM dim_week WHERE week_id = '{{ week_id }}'),
       (SELECT MIN(month_key) FROM dim_month WHERE month_id = '{{ month_id }}')
WHERE NOT EXISTS (SELECT 1 FROM dim_day WHERE day_id = '{{ day_id }}');
