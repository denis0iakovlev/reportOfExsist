import pandas as pd
import sys 
import os
import holidays

def read_excel_files(folder_path:str, date_col_name:int, id_col_name:int, full_name:int):
    # Список для хранения датафреймов с данными из каждого файла
    col_name = ["id", "date", "fio"]
    dataframes = pd.DataFrame(columns=col_name)
    # Проходим по всем файлам в директории
    for filename in os.listdir(folder_path):
        # Фильтруем файлы с расширениями Excel
        if filename.endswith(('.xls', '.xlsx')):
            file_path = os.path.join(folder_path, filename)
            try:
                # Считываем файл
                df = pd.read_excel(file_path, header=None)
                # Проверяем наличие нужных столбцов
                df_selected = df.iloc[:,[id_col_name ,date_col_name, full_name]]
                df_selected.columns = col_name 
                    # Извлекаем только столбцы 'Дата' и 'Id'
                dataframes = pd.concat([dataframes, df_selected], ignore_index=True)
            except Exception as e:
                print(f"Ошибка при чтении файла {filename}: {e}")
    dataframes[col_name[1]] = pd.to_datetime(dataframes[col_name[1]], errors='coerce')
    dataframes[col_name[0]] = pd.to_numeric(dataframes[col_name[0]])
    dataframes.dropna(subset=["date"], inplace=True)
    return dataframes

def calculate_time_deltas(df):
    # Группируем по id и дате без времени
    df["date_only"] = df["date"].dt.date
    result = df.groupby(["id", "date_only", "fio"]).agg(
        min_time=("date", "min"),
        max_time=("date", "max")
    ).reset_index()
    # Вычисляем разницу во времени
    result["time_delta"] = (result["max_time"] - result["min_time"]).dt.total_seconds() / 3600
    print(result.columns)
    return result

def create_exsist_df(raw_data:pd.DataFrame)->pd.DataFrame:
    # --- 1. Создаем пустой DataFrame с колонкой "ID" и столбцами для всех дней 2024 года ---
    # Генерируем даты от 2024-01-01 до 2024-12-31
    dates = pd.date_range(start='2024-01-01', end='2024-12-31')
    # Приводим даты к строковому формату "YYYY-MM-DD"
    date_columns = dates.strftime('%Y-%m-%d').tolist()
    # Формируем список столбцов: первый — "ID", затем все дни 2024 года
    columns = ['id', "Ф.И.О"] + date_columns
    # Создаем пустой DataFrame
    exsist_df = pd.DataFrame(columns=columns)

    for _, row in raw_data.iterrows():
        id_val = row['id']
        date_val = row['date_only']
        fio_val = row['fio']
        try:
            date_str = date_val.strftime('%Y-%m-%d')
            if id_val not in exsist_df['id'].values:
                new_row = {col:0 for col in exsist_df.columns}
                new_row['id'] = id_val
                new_row['Ф.И.О'] = fio_val
                exsist_df = pd.concat([exsist_df, pd.DataFrame(new_row, index=[0])], ignore_index=True)
            #Находим индекс строки соотвествующий id
            index_row_on_id= exsist_df.index[exsist_df['id'] == id_val][0]
            #Проверяем что год 24
            if date_str in exsist_df.columns:
                exsist_df.at[index_row_on_id, date_str] = row['time_delta']
            else:
                print(f'Диапазон дат включает в себя только 2024 год. Дата {date_str} к нему не относиться')
        except Exception as e:
            print(f"Ошибка при обработке данных {id_val} {fio_val} Текст.{e}")
    
    return exsist_df

def with_holidays(allDays:pd.DataFrame):
    # Список официальных праздничных дней в России в 2024 году
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
    columns = allDays.columns
    columns =columns.delete([0,1])
    print(columns)
    df_work_days = allDays.drop(columns=[col for col in columns if pd.to_datetime(col, errors='coerce') in excluded_dates], errors='ignore')
    df_free_days = allDays.drop(columns=[col for col in columns if  pd.to_datetime(col, errors='coerce') not in excluded_dates], errors='ignore')
    return df_work_days, df_free_days
    
def read_excel_and_get_cols(excel_file:str, inx_id:int, inx_sum_days:int)->pd.DataFrame:
     df = pd.read_excel(excel_file, header=None)
     df = df.iloc[:,[inx_id ,inx_sum_days]]
     df.columns =["ID", "days_count"]
     return df

def fill_summ_days(to_df:pd.DataFrame, from_df:pd.DataFrame, col_name_add:str, col_name_from:str):
    to_df[col_name_add] = None
    print(from_df.columns)
    to_df['id'] = pd.to_numeric(to_df["ID"], errors='coerce', downcast='unsigned').astype('Int64')
    for _, row in to_df.iterrows():
        #Получить id и найти в сводной таблице строку с таким же id
        id = row['id']
        if isinstance(id, int):
            filtered_df = from_df[from_df["id"] == id]
            if not filtered_df.empty:
                index_row_on_id= to_df.index[to_df['id'] == id][0]
                to_df.at[index_row_on_id, col_name_add] = filtered_df.iloc[0][col_name_from]
         
    to_df = to_df.drop(columns=['id'])
    return to_df

def calculate_exsisting_summ(df:pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df.dropna(subset=["date"], inplace=True)
    df["date_only"] = df["date"].dt.date
    result = df.groupby("id")["date_only"].nunique().reset_index()
    result.columns = ["ID", "days_count"]
    return result

def calc_summ_days(df:pd.DataFrame, name_col:str):
    df[name_col] = df.iloc[:, 2:-1].apply(lambda row: row[row > 4].count(), axis=1)
    return  df

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Укажите папку с файлами excel и полный путь к файлу с табелями')
        sys.exit(1)
    #Сгенерировать общую таблицу со всех файлов
    #general_df = read_excel_files(sys.argv[1], date_col_name=5, id_col_name=4, full_name=2)
    folder_with_excel = sys.argv[1]
    tabels_file = sys.argv[2]
    if not tabels_file.endswith(('.xls', '.xlsx')):
        print(f"Wrong argument with tabel data  {tabels_file}")
        sys.exit(1)
    general_df = read_excel_files(folder_with_excel, date_col_name=1, id_col_name=0, full_name=2)
    #Посчитать по каждому идентификатору часы по каждому дню
    ever_day = calculate_time_deltas(general_df)
    #Для проверки данных
    #ever_day.to_excel("./ever_day.xlsx")
    #Сгенерировать шаблон отчета, где каждый столбец это один день года
    exsist_df = create_exsist_df(raw_data=ever_day)
    splited_on_work_days = with_holidays(exsist_df)
    #
    calc_summ_days(splited_on_work_days[0], "Summ")
    calc_summ_days(splited_on_work_days[1], "Summ")
    calc_summ_days(exsist_df, "Summ")
    #
    reportPath= './report_of_exsist.xlsx'
    with pd.ExcelWriter(reportPath) as writer:
        exsist_df.to_excel(writer, sheet_name="Все дни", index=False)
        exsist_df[['id', "Ф.И.О"]].to_excel(writer, sheet_name="Пользователи", index=False)
        splited_on_work_days[0].to_excel(writer, sheet_name="Только рабочие", index=False)
        splited_on_work_days[1].to_excel(writer, sheet_name="Только праздничные", index=False)
    #
    report_with_summ = './summ_days.xlsx'
    df_tabel = read_excel_and_get_cols(tabels_file, inx_id=1, inx_sum_days=2)
    df_with_summ = fill_summ_days(df_tabel, exsist_df, col_name_add="Summ days 1C", col_name_from='Summ')
    df_with_summ.to_excel(report_with_summ)
    print(f"Результат сохранён в файл {reportPath}")
    sys.exit(0)


    

