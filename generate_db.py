import sys
from utils.parseUtils import read_excel_files, read_excel
from utils.db_fill_utils import add_departments, add_gates, add_types_gates, add_pass_types, add_person_list, add_gate_pass_registrations
from datetime import datetime
import pandas as pd

def generate_db(folder_path:str, gates_file:str):
    #Обработаем файл со списком проходных
    gate_extract_col = {'id':0, 'gate_name':1, 'gate_type':2}
    df_gates = read_excel(gates_file,gate_extract_col)
    get_types = df_gates['gate_type'].unique().tolist()
    add_types_gates(get_types)
    gates_list = list(df_gates.itertuples(index=False, name=None))
    add_gates(gates_list)
    print(df_gates)
    # Извлечть все данные из файлов excel  и записать данные в БД
    extracted_columns = {"division": 0,'dep_name':1, 'prof_name':3, "id":4, "date":5, "pass_type":6,"gate":7}
    df_exsist = read_excel_files(folder_path=folder_path, column_get=extracted_columns)
    df_exsist['date'] = pd.to_datetime(df_exsist['date'])
    print(df_exsist)
    departament_list = df_exsist['division'].unique().tolist()
    add_departments(department_names=departament_list)
    #Тип отметки на проходной Вход/Выход
    pass_type_list = df_exsist['pass_type'].unique().tolist()
    add_pass_types(pass_type_list)
    #Теперь заполлняем таблицу сотрудников
    person_df =  df_exsist[['id', 'dep_name', 'prof_name']]
    person_list = list(person_df.itertuples(index=False, name=None, ))
    add_person_list(person_list)
    #Заполняем таблицу регистрации на проходной 
    gate_pass_list = list(df_exsist[['division', 'id', 'date', 'pass_type', 'gate']].itertuples(index=False, name=None))
    add_gate_pass_registrations(gate_pass_list)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Укажите папку с файлами excel и полный путь к файлу с данными о проходных')
        sys.exit(1)
    folder_excel = sys.argv[1]
    gates_excel = sys.argv[2]
    generate_db(folder_excel,gates_excel )
