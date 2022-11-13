from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

default_args={
    'owner':'Birhanug',
    'retries':5,
    'retry_delay':timedelta(minutes=3)
}

def modify_raw_data(location):
    updated_lines=""
    with open(location, 'r', encoding='ISO-8859-1') as f:
        lines = f.readlines()
        for index , line in enumerate(lines):
            each_line = line.split(';')
            if index != 0:
                updated_lines += ";".join(each_line[0:10]) + ";" + "_".join(each_line[10:])
            else:
                updated_lines += ";".join(each_line[:len(each_line)-1]) + ";" + "time" + ";" + "other_data" + "\n" 
    with open('./data/transformed_dataset', "w") as f:
        f.writelines(updated_lines)


with DAG(
    dag_id='load_raw_data_to_db',
    default_args=default_args,
    description='extract and load raw data to db',
    start_date=datetime(2022,9,21),
    schedule_interval='@once'
)as dag:
    read_data = PythonOperator(
        task_id='migrate',
        python_callable=modify_raw_data,
        op_kwargs={
            "location": "./data/20181024_d2_0830_0900.csv"
        }
    )
    create_db = PostgresOperator(
        task_id='create_database',
        postgres_conn_id='postgres_default',
        sql='/sql_db/db.sql',
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql='/sql_db/table.sql',
    )
    load_data = PostgresOperator(
        task_id='load_dataset',
        postgres_conn_id='postgres_default',
        sql='/sql_db/load_data.sql',
    )

    read_data >> create_db >> create_table >> load_data