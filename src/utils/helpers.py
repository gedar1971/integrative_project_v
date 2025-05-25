import json
import os
import io
import pandas as pd
import pickle
from datetime import datetime


def create_file(data, filename, file_format='json'):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if file_format == 'json':
        with open(filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Archivo JSON '{filename}' generado exitosamente.")

    elif file_format == 'xlsx':
        if isinstance(data, pd.DataFrame):
            data.to_excel(filename, index=False)
            print(f"Archivo Excel '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como Excel.")

    elif file_format == 'csv':
        if isinstance(data, pd.DataFrame):
            data.to_csv(filename, index=False)
            print(f"Archivo CSV '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como CSV.")
            
    elif file_format == 'pkl' or file_format == 'pickle':
        with open(filename, 'wb') as pickle_file:
            pickle.dump(data, pickle_file)
        print(f"Archivo pickle '{filename}' generado exitosamente.")
        
    elif file_format == 'txt':
        if isinstance(data, pd.DataFrame):
            data.to_txt(filename, index=False)
            print(f"Archivo TXT '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como TXT.")

    else:
        raise ValueError(f"Formato de archivo no soportado: {file_format}")
    

def convert_to_numeric(df, numeric_columns):
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    return df

def convert_to_lowercase(df, text_lowercase_columns):
    for column in text_lowercase_columns:
        df[column] = df[column].str.lower()
    return df