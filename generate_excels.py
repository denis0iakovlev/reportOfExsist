import sys
import os
import pandas as pd
import random
from datetime import datetime, timedelta
from generate_db import generate_db

gate_name = ["gate 1","gate 2", "gate 3", 'gate 4' , 'gate 45', 'gate78']
type_registry = ['Вход', 'Выход']

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
    for _ in range(random_num_rec):
        # Генерируем случайное количество дней для сдвига от start_date
        random_days = random.randint(0, delta_days)
        #Записываем как миниму два времени прихода и ухода
        r_minutes = random.randint(0,60)
        r_hour = random.randint(0, 12)
        random_date_input = start_date + timedelta(days=random_days, hours=r_hour, minutes=r_minutes)
        r_minutes = random.randint(0,60)
        r_hour = random.randint(12, 24)
        random_date_output = start_date + timedelta(days=random_days, hours=r_hour, minutes=r_minutes)
        # Форматируем дату в формат "YYYY-MM-DD"
        formatted_date = random_date_input.strftime('%Y-%m-%d %H:%M')
        random_gate = random.randint(0,5)
        res.append((random_number, formatted_date, f"{first_names[random_fn]} {last_names[random_ln]}", type_registry[0], gate_name[random_gate]))
        formatted_date = random_date_output.strftime('%Y-%m-%d %H:%M')
        res.append((random_number, formatted_date, f"{first_names[random_fn]} {last_names[random_ln]}", type_registry[1], gate_name[random_gate]))
    
    return res

def generate_random_data(n):
    """
    Генерирует DataFrame из n строк, каждая строка содержит случайное число и случайную дату.
    """
    # Создаем список кортежей, где каждый кортеж содержит (случайное число, случайная дата)
    data=[]
    for _ in range(n):
        tuple_list = generate_random_number_and_date()
        add = [{ "Department": "Kalash" ,"data_1": None, "Fio":  t[2], "data 2": None , "ID": t[0] , "Дата": t[1] , "type_data": t[3], "gate": t[4]} for t in tuple_list]
        data.extend(add)
         
    # Преобразуем список в DataFrame с нужными заголовками столбцов
    df = pd.DataFrame(data)
    df['ID'] = df['ID'].astype('int64')
    return df
def generate_df_tabel(df:pd.DataFrame):
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
    # out_tabel_file = "./tabel_data.xlsx"
    # df_tabel = generate_df_tabel(df_random)
    # df_tabel.to_excel(out_tabel_file)
    print(f"Данные сохранены в файл: {output_file}")
    gate_df = pd.DataFrame({
        "ID":[10,20,23,24,35,56],
        "NameGate":gate_name,
        'Type Gate':['внешний', 'внешний', 'внутренний','внутренний', 'внутренний','внешний']
    })
    gate_df.to_excel('gates/gates_data.xlsx', header=None, index=False)
    generate_db(folder_path='./test_data', gates_file='gates/gates_data.xlsx')
