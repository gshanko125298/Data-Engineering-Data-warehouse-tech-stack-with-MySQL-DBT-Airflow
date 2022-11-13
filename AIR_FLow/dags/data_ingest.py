from airflow import DAG 
import pandas as pd
import sys
import os
import pandas as pd
import json
print(os.path.abspath("working.............................."))
sys.path.append(os.path.abspath("data"))
from sqlalchemy import Numeric
from datetime import datetime as dt
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# defaults
default_args = {
    'owner': 'Genet',
    'depends_on_past': False,
    'email': ['gdekebo2020@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': dt(2022, 9, 22),
    'retry_delay': timedelta(minutes=5)
}
 
#Airflow
with DAG(
    dag_id='Local_ELT_DAG',
    default_args=default_args,
    description='Upload data from CSV to Postgres and Transform it with dbt',
    schedule_interval="@daily",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
) as pg_dag:

 table_creator = PostgresOperator(
    task_id="create_table", 
    postgres_conn_id="postgres_server",
    sql = '''
        CREATE TABLE  IF NOT EXISTS pnuema( 
            id serial primary key,
            track_id numeric, 
            type text not null, 
            traveled_d double precision DEFAULT NULL,
            avg_speed double precision DEFAULT NULL, 
            lat double precision DEFAULT NULL, 
            lon double precision DEFAULT NULL, 
            speed double precision DEFAULT NULL,    
            lon_acc double precision DEFAULT NULL, 
            lat_acc double precision DEFAULT NULL, 
            time double precision DEFAULT NULL
        );
    '''
    )
data_loader = PostgresOperator(
    sql="sql_data/data_load.sql",
    task_id="load_data",
    postgres_conn_id="postgres_server",
)
#transform data
DBT_PROJECT_DIR = "/opt/airflow/dbt_demo"
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"cd ~/dbt_demo && ~/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR}",
 )

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
 )

dbt_doc_generate = BashOperator(
    task_id="dbt_doc_generate", 
    bash_command="dbt docs generate --profiles-dir /opt/airflow/dbt --project-dir "
                    "/opt/airflow/dbt_demo"
 )

email_report = EmailOperator(
    task_id='send_email',
    to='birhanugebisa@gmail.com',
    subject='Daily report generated',
    html_content=""" <h3>Congratulations! ELT process is completed.</h3> """
 )

   #Task

table_creator >> data_loader >> dbt_run >> dbt_test >> dbt_doc_generate >> email_report