from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json

from data_clean_sample_superstore import CleanSampleSuperstore
from mysql_db import MySqlDB


default_args = {
    'depends on past': False,
    'email': ['herbertab@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('Sample_Superstore', description="Pipeline de tratamento dos dados do arquivo Sample_Superstore",
        schedule_interval=None, start_date=datetime(2023,7,31),
        catchup=False, default_args=default_args)
        

def clean_data():
    df = pd.read_csv(Variable.get('file_path_in'), encoding='latin1')
    df = CleanSampleSuperstore(df)
    df.to_json(Variable.get('file_path_out'))
    df.to_csv(Variable.get('file_path_cleaned'), encoding='latin1')

def get_data(**kwargs):
    with open(Variable.get('file_path_out')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push(key='Order ID', value=data['Order ID'])

def create_dCustomer():
    db = MySqlDB('mysql', 'airflow', 'airflow', 'airflow')
    fields = {
        'Customer_ID': 'VARCHAR(20)',
        'Customer_Name': 'VARCHAR(100)',
        'Segment': 'VARCHAR(50)',
        'Country': 'VARCHAR(50)',
        'City': 'VARCHAR(80)',
        'State': 'VARCHAR(50)',
        'Region': 'VARCHAR(30)'
    }    
    db.create_table('dCustomer', **fields)

def populate_dCustomer():
    db = MySqlDB('mysql', 'airflow', 'airflow', 'airflow')
    df = pd.read_csv(Variable.get('file_path_cleaned'), encoding='latin1')
    df = df[['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Region']]
    db.insert_data('dCustomer', df)


clean_data_task = PythonOperator(task_id="clean_data_task", python_callable=clean_data, dag=dag)
get_data_task = PythonOperator(task_id="get_data_task", python_callable=get_data, provide_context=True, dag=dag)
create_dCustomer_table_task = PythonOperator(task_id="create_dCustomer_table_task", python_callable=create_dCustomer, dag=dag)
populate_dCustomer_task = PythonOperator(task_id="populate_dCustomer_task", python_callable=populate_dCustomer, dag=dag)

clean_data_task >> get_data_task >> create_dCustomer_table_task >> populate_dCustomer_task