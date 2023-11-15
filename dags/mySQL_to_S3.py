from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator



dag = DAG(
    dag_id = 'MySQL_to_S3',
    start_date = datetime(2023,11,13), 
    schedule = '10 9 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
)

sql = "SELECT * FROM transaction_history;"


mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3',
    query = sql,
    s3_bucket = 'jongjun',
    s3_key = 'transaction_history.csv',
    sql_conn_id = "mysql_localhost",
    aws_conn_id = "aws_s3",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)
