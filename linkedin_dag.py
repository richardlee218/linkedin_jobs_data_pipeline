import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
}

today_timestamp = pd.Timestamp.today()
today_date_int = today_timestamp.strftime('%Y%m%d')
today_time_int = today_timestamp.strftime('%H%M')

def process_datetime(ti):
    dt = ti.xcom_pull(task_ids=['get_datetime'])
    if not dt:
        raise Exception('No datetime value.')
    dt = str(dt[0]).split()
    return {
        'year': int(dt[-1]),
        'month': dt[1],
        'day': int(dt[2]),
        'time': dt[3],
        'day_of_week':dt[0]
    }


def save_datetime(ti):
    dt_process = ti.xcom_pull(task_ids=['process_datetime'])
    if not dt_process:
        raise Exception('No processed datetime value.')
    df = pd.DataFrame(dt_process)
    csv_filename = f"datetimes_{today_date_int}_{today_time_int}.csv"
    csv_path = os.path.join('/to/your/path/', csv_filename)
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'
    else:
        df_header = True
        df_mode = 'w'
    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)

with DAG(
    dag_id='execute_linkedin_data_pyscript',
    description='A simple DAG to execute pyscript that fetch jobpost data from linkedin and saves output as csv',
    default_args=default_args,
    catchup=False,
    schedule='@once'
) as dag:
    # 1. Get current datetime
    task_get_datetime = BashOperator(
        task_id='get_datetime',
        bash_command='date'
    )

    # 2 Process current datetime
    task_process_datetime = PythonOperator(
        task_id='process_datetime',
        python_callable=process_datetime
    )

    # 3 Save processed datetime
    task_save_datetime = PythonOperator(
        task_id='save_datetime',
        python_callable=save_datetime
    )

    # 4 Execute Jobpost pyscript thru Bash
    execute_job_script = BashOperator(
        task_id='run_jobpost_script',
        bash_command='pyenv exec python3 /to/your/path/linkedin_jobpost_script.py',
    )

    # 5 Execute File uplaod pyscript thru Bash
    execute_upload_script = BashOperator(
        task_id='run_upload_jobpost_script',
        bash_command='pyenv exec python3 /to/your/path/read_csv_load_to_db.py',
    )

    task_get_datetime >> task_process_datetime >> task_save_datetime >> execute_job_script >> execute_upload_script
