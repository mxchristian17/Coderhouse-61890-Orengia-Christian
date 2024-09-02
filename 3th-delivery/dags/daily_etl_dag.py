from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
import os

# Añadir el path de tu script
sys.path.insert(0, '/app/dags')

# Importar la función de tu script
from etl import main_function  # Asegúrate que la función principal se llama `main_function`

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'Christian Orengia',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

# Crear el DAG
with DAG('daily_etl_dag',
         default_args=default_args,
         description='DAG para ejecutar el script ETL diariamente',
         schedule_interval='@daily',
         catchup=False) as dag:

    # Crear la tarea para ejecutar el script Python
    run_etl = PythonOperator(
        task_id='run_etl_script',
        python_callable=main_function,  # Llama a la función que ejecuta el ETL
    )

    run_etl
