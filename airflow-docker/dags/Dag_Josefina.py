
from datetime import timedelta,datetime
from pathlib import Path 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from  entrega_final import obtener_informacion_inflacion, main, send_email 
dag_path = os.getcwd()

default_args = {
    'Owner': 'josefinacarlos',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

inflation_dag = DAG(
    dag_id='inflation_dag',
    default_args=default_args,
    description='Agrega datos de la inflacion de eeuu',
    schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id='recolectar_data_ETL',
    python_callable=obtener_informacion_inflacion,
    dag=inflation_dag,
)

task_2 = PythonOperator(
    task_id='main_function',
    python_callable=main,
    dag=inflation_dag,
)

task_3 = PythonOperator(
    task_id='send_email',
    python_callable=send_email,  
    dag=inflation_dag,  
)

task_1 >> task_2 >> task_3 