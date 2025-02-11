import os
import pandas as  pd

# Извлекает из папки все excel файлы считывает их и обьединяет в один DataFrame
#read_excel_files - путь к папке с файлами excel
# словрь гдк ключ значение имени колонки выходногофрейма а значение индекс колонки
def read_excel_files(folder_path:str, column_get:dict)->pd.DataFrame:
    # Список для хранения датафреймов с данными из каждого файла
    col_name = column_get.keys()
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
                df_selected = df.iloc[:, [val for val in column_get.values()]]
                df_selected.columns = col_name 
                    # Извлекаем только столбцы 'Дата' и 'Id'
                dataframes = pd.concat([dataframes, df_selected], ignore_index=True)
            except Exception as e:
                print(f"Ошибка при чтении файла {filename}: {e}")
    return dataframes
def read_excel(full_excel_name:str, col_index_dict:dict)->pd.DataFrame:
    col_name = col_index_dict.keys()
    sel_indexes = [value for value in col_index_dict.values()]
    try:
        # Считываем файл
        df = pd.read_excel(full_excel_name, header=None)
        # Проверяем наличие нужных столбцов
        df = df.iloc[:, sel_indexes]
        df.columns = col_name 
        # Извлекаем только столбцы 'Дата' и 'Id'
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла {full_excel_name}: {e}")