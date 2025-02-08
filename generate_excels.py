import sys
import os
import pandas as pd
import random
from datetime import datetime, timedelta


def generate_random_number_and_date():
    """
    Генерирует случайное пятизначное число и случайную дату в диапазоне 2024 года.
    """
    # Случайное пятизначное число (от 10000 до 99999)
    
    # Задаем диапазон дат для 2024 года
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    first_names = ["Ivan", "Petr", "Sergey", "Alexey", "Dmitry"]
    last_names = ["Ivanov", "Petrov", "Sidorov", "Smirnov", "Volkov"]
    random_fn = random.randint(0,4)
    random_ln = random.randint(0,4)
    random_num_rec = random.randint(10, 90)
    # Вычисляем общее количество дней в диапазоне
    delta_days = (end_date - start_date).days
    res = []
    random_number = random.randint(10000, 99999)
    for i in range(random_num_rec):
        # Генерируем случайное количество дней для сдвига от start_date
        random_days = random.randint(0, delta_days)
        #Записываем как миниму два времени прихода и ухода
        random_times_picks = random.randint(2, 4)
        for _ in range(random_times_picks):
            r_hour = random.randint(6, 19)
            r_minutes = random.randint(0,60)
            random_date = start_date + timedelta(days=random_days, hours=r_hour, minutes=r_minutes)
            # Форматируем дату в формат "YYYY-MM-DD"
            formatted_date = random_date.strftime('%Y-%m-%d %H:%M')
            res.append((random_number, formatted_date, f"{first_names[random_fn]} {last_names[random_ln]}"))
    
    return res

def generate_random_data(n):
    """
    Генерирует DataFrame из n строк, каждая строка содержит случайное число и случайную дату.
    """
    # Создаем список кортежей, где каждый кортеж содержит (случайное число, случайная дата)
    data=[]
    for _ in range(n):
        data.extend(generate_random_number_and_date())
    # Преобразуем список в DataFrame с нужными заголовками столбцов
    df = pd.DataFrame(data, columns=['ID', 'Дата', "ФИО"])
    df['ID'] = df['ID'].astype('int64')
    return df
def genarte_df_tabel(df:pd.DataFrame):
    df["Дата"] = pd.to_datetime(df["Дата"], errors='coerce')
    df.dropna(subset=["Дата"], inplace=True)
    df["date_only"] = df["Дата"].dt.date
    result = df.groupby("ID")["date_only"].nunique().reset_index()
    result.columns = ["ID", "days_count"]
    return result

if __name__ == '__main__':

    # Задаем количество генерируемых строк
    n_rows = 10  # можно изменить на любое другое количество
    # Генерируем DataFrame с данными
    df_random = generate_random_data(n_rows)
    # 
    
    # Сохраняем DataFrame в Excel-файл (без индексов)
    output_file = 'test_data/random_data.xlsx'
    df_random.to_excel(output_file, index=False, header=None)
    out_tabel_file = "./tabel_data.xlsx"
    df_tabel = genarte_df_tabel(df_random)
    df_tabel.to_excel(out_tabel_file)
    print(f"Данные сохранены в файл: {output_file}")