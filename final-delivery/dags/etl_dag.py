from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags_modules.etl import setup_database, extract_data, transform_data, load_data
from dags_modules.notification_manager import notify

# Function to set up the database
def run_database_setup():
    setup_database()

def run_extract(**kwargs):
    population_data, weather_data = extract_data()
    kwargs['ti'].xcom_push(key='population_data', value=population_data)
    kwargs['ti'].xcom_push(key='weather_data', value=weather_data)

def run_transform(**kwargs):
    population_data = kwargs['ti'].xcom_pull(key='population_data', task_ids='extract_task')
    weather_data = kwargs['ti'].xcom_pull(key='weather_data', task_ids='extract_task')
    population_df, weather_df = transform_data(population_data, weather_data)
    kwargs['ti'].xcom_push(key='population_df', value=population_df)
    kwargs['ti'].xcom_push(key='weather_df', value=weather_df)

def run_load(**kwargs):
    population_df = kwargs['ti'].xcom_pull(key='population_df', task_ids='transform_task')
    weather_df = kwargs['ti'].xcom_pull(key='weather_df', task_ids='transform_task')
    load_data(population_df, weather_df)

def notify_failure(context):
    exception = context.get('exception')
    error_message = str(exception) if exception else "Unknown error occurred"
    notify("failure", error_message)
    print(error_message)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure,
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A DAG to run ETL in three steps: extract, transform, load',
    schedule_interval='* */4 * * *',
    start_date=days_ago(1),
    catchup=True,
)

# Define the ETL task
setup_database_task = PythonOperator(
    task_id='setup_database_task',
    python_callable=run_database_setup,
    provide_context=True,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=run_extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=run_transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=run_load,
    provide_context=True,
    dag=dag,
)

send_success_notification = PythonOperator(
    task_id="notifier",
    python_callable=lambda: notify("success"),
    dag=dag,
    trigger_rule='all_success' # This task runs only if all upstream tasks are successful
)

# Define task sequence
setup_database_task >> extract_task >> transform_task >> load_task >> send_success_notification