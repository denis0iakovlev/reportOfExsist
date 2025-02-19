from utils.db_get_utils import get_filtered_gate_pass_times
import pandas as pd
from datetime import datetime, time, timedelta
import shutil

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
    """Вычисляет рабочее время по первому входу и последнему выходу, учитывая переход дней и множественные записи.
    Если день содержит только выходы, проверяет предыдущий день на наличие входа и учитывает рабочее время от полуночи.
    Разбивает последовательности ВХОД-ВЫХОД на группы."""
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by=['person_id', 'timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['group_id'] = (df['pass_type'] != df['pass_type'].shift()).cumsum()
    work_times = []

    for (person_id,date), group in df.groupby(['person_id', 'date']):
        group = group.sort_values(by='timestamp')
        start_time = None
        end_time = None
        isHoliday = group.iloc[0]['isHoliday']
        #Разбиваем кокретный день конкретного пользователя на входы-выходы
        for _, sub_group in group.groupby('group_id'):
            sub_group = sub_group.sort_values(by='timestamp')
            type_pass_group = sub_group.iloc[0]['pass_type']
            if type_pass_group == 'ВХОД':
                #проверяем что предыдущая записи была ВЫХОД
                start_time = sub_group.iloc[0]['timestamp']
            else:
                st_tm_is_set = True
                if start_time is None:
                    start_time = datetime.combine(date, time(0,0,0))
                    st_tm_is_set = False
                end_time = sub_group.iloc[-1]['timestamp']
                work_duration = (end_time - start_time).total_seconds() / 3600
                if  len(work_times) == 0 or work_times[-1]['pass_type'] == 'ВХОД' or st_tm_is_set  :
                    work_times.append({
                        'person_id': person_id, 
                        'date': date, 
                        'pass_type':type_pass_group,
                        'work_hours': work_duration,
                        'isHoliday': isHoliday,
                    })
                start_time = None
                end_time = None
        if start_time is not None and end_time is None:
            work_duration = ( datetime.combine(date, time(23,59,59)) - start_time).total_seconds() / 3600
            work_times.append({
                    'person_id': person_id, 
                    'date': date, 
                    'pass_type':type_pass_group,
                    'work_hours': work_duration,
                    'isHoliday': isHoliday,
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


def normalize_and_calculate_total_work(df: pd.DataFrame) -> pd.DataFrame:
    """Нормализует рабочее время (ограничивает 8 часами) и считает общее рабочее время и количество дней."""
    df_normalized = df[['person_id', 'work_hours']].copy()
    df_normalized['work_days'] = 1
    df_normalized['total_hours'] = df['work_hours'].map(lambda x: min(x, 8) if pd.notna(x) else x)
    #df_normalized['total_days'] = df_normalized['total_hours']/ 8
    df_normalized = df_normalized.groupby('person_id', as_index=False).sum()
    df_normalized['total_days'] = df_normalized['total_hours']/8
    return df_normalized.reset_index()



def update_excel_with_work_data(excel_path: str, work_df: pd.DataFrame, hours_col_idx: int, days_col_idx: int, skip_row:int =10 ):
    """Обновляет Excel-файл, заполняя суммы отработанных часов и дней по ID сотрудников."""
    df_excel = pd.read_excel(excel_path)
    
    work_dict = work_df.set_index('person_id').to_dict()
    
    for index, row in df_excel.iterrows():
        person_id = row.iloc[0]
        if person_id in work_dict['total_hours']:
            df_excel.at[index, df_excel.columns[hours_col_idx]] = work_dict['total_hours'][person_id]
        if person_id in work_dict['total_days']:
            df_excel.at[index, df_excel.columns[days_col_idx]] = work_dict['total_days'][person_id]
    
    return df_excel


if __name__ == '__main__':
    yanuar_pass_list = get_filtered_gate_pass_times()
    all_year_db_records = pd.DataFrame(yanuar_pass_list)
    work_times = calculate_daily_work_time_v2(all_year_db_records)
    annual_repo_df = create_anual_report(work_times)
    excel_path = 'db_reports/annual_repo_df.xlsx'
    annual_repo_df.to_excel('db_reports/annual_repo_df.xlsx')
    print(f"Отчет учета времени по непрерывному рабочему создан. Путь к отчету {excel_path}")
    #Версия без разделения по полонучи
    work_times_v2 = calculate_daily_work_time_on_nearest(all_year_db_records)
    print(work_times_v2)
    annual_repo_df_v2 = create_anual_report(work_times_v2)
    excel_path = 'db_reports/annual_repo_df_v2.xlsx'
    annual_repo_df_v2.to_excel('db_reports/annual_repo_df_v2.xlsx')
    print(f"Отчет учета времени с разбивкой по дням создан. Путь к отчету {excel_path}") 
    share_excel_path = 'Свод_primer.xlsx'
    total_days_and_hours_df = normalize_and_calculate_total_work(work_times_v2)
    excel_path  = 'db_reports/share_report.xlsx'
    shutil.copy(share_excel_path, excel_path)
    shared_report = update_excel_with_work_data(excel_path=excel_path, work_df=total_days_and_hours_df, hours_col_idx=11, days_col_idx=10)
    shared_report.to_excel(excel_path, index=False, header=False)
    print(f"Сводный отчет создан. Путь к отчету {excel_path}")


