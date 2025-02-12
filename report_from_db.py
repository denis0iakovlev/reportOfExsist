from utils.db_get_utils import get_filtered_gate_pass_times
import pandas as pd
from datetime import datetime, time, timedelta

def calculate_daily_work_time_on_nearest(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['person_id', 'timestamp'])
    work_times = []
    
    for person_id, group in df.groupby('person_id'):
        entries = group[group['pass_type'] == 'ВХОД']['timestamp'].tolist()
        exits = group[group['pass_type'] == 'ВЫХОД']['timestamp'].tolist()
        while entries:
            entry_time = entries.pop(0)
            limit_time = entry_time + timedelta(hours=24)
            exit_time = next((e for e in exits if e > entry_time and e < limit_time  ), None)
            
            if exit_time is None:
                exit_time = limit_time
            else:
                exits.remove(exit_time)
            
            work_duration = (exit_time - entry_time).total_seconds() / 3600
            
            work_times.append({
                'person_id': person_id, 
                'date': entry_time.date(), 
                'isHoliday':group.loc[group['timestamp'] == entry_time, 'isHoliday'].values[0],
                'work_hours': work_duration
            })
    
    result_df = pd.DataFrame(work_times)
    return result_df.sort_values(by=['date'])


def calculate_daily_work_time_v2(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df = df.sort_values(by=['person_id', 'timestamp'])
    work_times = []
    
    for (person_id,date), group in df.groupby(['person_id', 'date']):
        entries = group['timestamp'].tolist()
        while entries:
            entry_time = entries.pop(0)
            start_time = datetime.combine(entry_time.date(), time(0, 0, 0))
            end_time = datetime.combine(entry_time.date(), time(23, 59, 59))
            type_pass = group.loc[group['timestamp'] == entry_time,'pass_type' ].values[0]
            #Обрабатываем случий если вход и текущий индекс не последний
            if type_pass == 'ВХОД' : 
                start_time = entry_time
                #Если есть следующий элемент то пытаемся получить конечную вермя пребывания
                while entries:
                    #Получить следующующую дату 
                    next_time = entries[0]
                    type_next_time = group.loc[group['timestamp'] == next_time,'pass_type' ].values[0]
                    if type_next_time == 'ВЫХОД':
                        end_time = entries.pop(0)
                        break
                    else:
                        entries.pop(0)
            else: # если выход 
                end_time = entry_time
                while entries:
                    #Получить следующующую дату 
                    next_time = entries[0]
                    type_next_time = group.loc[group['timestamp'] == next_time,'pass_type' ].values[0]
                    if type_next_time == 'ВХОД':
                        end_time = entries.pop(0)
                        break
                    else:
                        entries.pop(0)
            
            work_duration = (end_time - start_time).total_seconds() / 3600
            
            work_times.append({
                'person_id': person_id, 
                'date': entry_time.date(), 
                'isHoliday':group.loc[group['timestamp'] == entry_time, 'isHoliday'].values[0],
                'work_hours': work_duration
            })
    
    result_df = pd.DataFrame(work_times)
    return result_df.sort_values(by=['date'])


def create_anual_report(df: pd.DataFrame)-> pd.DataFrame:
    """Генерирует годовой отчет по отработанному времени."""
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['day'] = pd.to_datetime(df['date']).dt.day
    df['month_name'] = df['date'].apply(lambda x: x.strftime('%B').capitalize())
    month_translation = {
        'January': 'Январь', 'February': 'Февраль', 'March': 'Март', 'April': 'Апрель',
        'May': 'Май', 'June': 'Июнь', 'July': 'Июль', 'August': 'Август',
        'September': 'Сентябрь', 'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
    }
    df['month_name'] = df['month_name'].map(month_translation)
    df['formatted_date'] = df['day'].astype(str) + '-' + df['month'].astype(str)

    annual_report = df.pivot_table(index='person_id', columns=['month', 'day'], values='work_hours', aggfunc='sum', fill_value=0)
    print(annual_report)
    monthly_totals = df.groupby(['person_id', 'month_name'])['work_hours'].sum().unstack(fill_value=0)
    workdays_totals = df[df['isHoliday'] == False].groupby(['person_id', 'month_name'])['work_hours'].sum().unstack(fill_value=0)
    holidays_totals = df[df['isHoliday'] == True].groupby(['person_id', 'month_name'])['work_hours'].sum().unstack(fill_value=0)

    annual_report.columns = [f"{datetime(year=2024,month=col[0], day=col[1]).strftime('%d/%m')}" for col in annual_report.columns]
    
    for month_name in df['month_name'].unique():
        last_day_col = df[df['month_name'] == month_name]['day'].max()
        month = df[df['month_name'] == month_name]['month'].max()
        name_col = f"{datetime(year=2024,month=month, day=last_day_col).strftime('%d/%m')}"
        if name_col in annual_report.columns:
            annual_report.insert(annual_report.columns.get_loc(name_col) + 1, f"{month_name} Раб.", workdays_totals.get(month_name, 0))
            annual_report.insert(annual_report.columns.get_loc(name_col) + 2, f"{month_name} Неуроч.", holidays_totals.get(month_name, 0))
            annual_report.insert(annual_report.columns.get_loc(name_col) + 3, f"{month_name} Общее", monthly_totals.get(month_name, 0))
    return annual_report

if __name__ == '__main__':
    yanuar_pass_list = get_filtered_gate_pass_times()
    all_year_db_records = pd.DataFrame(yanuar_pass_list)
    work_times = calculate_daily_work_time_v2(all_year_db_records)
    annual_repo_df = create_anual_report(work_times)
    annual_repo_df.to_excel('db_reports/annual_repo_df.xlsx')
    #Версия без разделения по полонучи
    work_times_v2 = calculate_daily_work_time_on_nearest(all_year_db_records)
    print(work_times_v2)
    annual_repo_df_v2 = create_anual_report(work_times_v2)
    annual_repo_df_v2.to_excel('db_reports/annual_repo_df_v2.xlsx')
    print("Отчеты сгенерированы")