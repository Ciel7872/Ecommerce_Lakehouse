import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST_INIT = os.getenv('DB_HOST_INIT')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Crear el engine de SQLAlchemy de forma segura
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST_INIT}:{DB_PORT}/{DB_NAME}')

# Diccionario con el nombre del archivo y el nombre de la tabla
datasets = {
    'olist_customers_dataset.csv': 'customers',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products'
}

#ruta cvs
data_path = 'data/raw/'

def load_data_to_postgres():
    print("Iniciando la carga de datos a PostgreSQL...")
    
    for file_name, table_name in datasets.items():
        file_path = os.path.join(data_path, file_name)
        
        if os.path.exists(file_path):
            print(f"Leyendo {file_name}...")
            df = pd.read_csv(file_path)
            
            print(f"Insertando en la tabla '{table_name}'...")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Tabla '{table_name}' cargada exitosamente.\n")
        else:
            print(f"Advertencia: No se encontró el archivo {file_path}")


if __name__ == "__main__":
    load_data_to_postgres()