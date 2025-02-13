from airflow import DAG
from airflow.operators.bash_operator  import BashOperator
from airflow.operators.python_operator  import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    "owner":'Celso Cazetto',
    "start_date":datetime(2025,2,1),
    "retries":1,
    "retry_delay":timedelta(minutes=1)
}

dag = DAG(
    "call_omdb_api",
    default_args=default_args,
    schedule_interval="* * * * *",  # Este agendamento deve ser ajustado conforme necessário
    catchup=False
)

VENV_PATH = "/home/airflow/venv/omdb_env/bin/activate"

task = BashOperator(
    task_id='get_omdb_filme',
    bash_command=f'source {VENV_PATH} && python {os.path.join(os.getcwd(), 'pipeline', 'main.py')}',
    dag=dag
)