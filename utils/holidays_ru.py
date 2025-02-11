import pandas as pd
from datetime import datetime
def get_holidays_days():
    holiday_dates =  [
    "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08", # Новогодние каникулы и Рождество
    "2024-02-23", # День защитника Отечества
    "2024-03-08", # Международный женский день
    "2024-05-01", # Праздник Весны и Труда
    "2024-05-09", # День Победы
    "2024-06-12", # День России
    "2024-11-04"  # День народного единства
    ]
     
    weekends = pd.date_range(start=f"2024-01-01", end=f"2024-12-31", freq='W-SAT').tolist() + \
               pd.date_range(start=f"2024-01-01", end=f"2024-12-31", freq='W-SUN').tolist()
    excluded_dates = set(weekends).union([pd.to_datetime(strData) for strData in holiday_dates])
    return [ts.to_pydatetime().date() for ts in excluded_dates]