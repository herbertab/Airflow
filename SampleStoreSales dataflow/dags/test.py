from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag = DAG('TESTE', description="Testando python operator",
        schedule_interval=None, start_date=datetime(2023,7,31),
        catchup=False)
        


task1 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_local',
    sql='create table if not exists teste(id int);',
    dag=dag
)