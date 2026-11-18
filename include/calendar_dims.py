"""Calendar values for one date (year, month, week, day), passed to the dim_year/month/week/day SQL."""
from datetime import date, timedelta


def parts(ds):
    d = date.fromisoformat(str(ds))
    monday = d - timedelta(days=d.weekday())
    iso = d.isocalendar()
    return {
        "year_id": f"{d.year}", "year_no": d.year,
        "month_id": d.strftime("%Y%m"), "month_no": d.month, "month_nm": d.strftime("%B"),
        "quarter_no": (d.month - 1) // 3 + 1,
        "week_id": f"{iso[0]}W{iso[1]:02d}", "week_no": iso[1],
        "week_start": monday.isoformat(), "week_end": (monday + timedelta(days=6)).isoformat(),
        "day_id": d.strftime("%Y%m%d"), "weekday_nm": d.strftime("%A"), "weekend": "TRUE" if d.weekday() >= 5 else "FALSE",
    }
