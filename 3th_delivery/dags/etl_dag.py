from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags_modules.etl import etl_process

def run_etl_script():
    etl_process()

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='Un DAG para ejecutar ETL cada día',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Definición de la tarea
run_etl = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl_script,
    dag=dag,
)

# Define la secuencia de tareas (si hay más tareas, las puedes agregar aquí)
run_etl
