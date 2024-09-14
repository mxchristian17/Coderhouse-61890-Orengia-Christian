from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags_modules.etl import etl_process
from dags_modules.notification_manager import notify

def run_etl_script():
    etl_process()
# Function to send failure notification with error details
def notify_failure(context):
    exception = context.get('exception')  # Capture the exception from the task context
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
    'on_failure_callback': notify_failure,  # Trigger notification on failure
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A DAG to run ETL every 10 minutes',
    schedule_interval='*/10 * * * *',
    start_date=days_ago(1),
    catchup=True,
)

# Define the ETL task
run_etl = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl_script,
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
run_etl >> send_success_notification