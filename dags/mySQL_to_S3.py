from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.operators.mysql import MySqlOperator


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_access')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


dag = DAG(
    dag_id = "MySQL_to_S3",
    start_date = datetime(2023,11,14),
    schedule = timedelta(minutes = 1),
    catchup = False,
    tags = ['S3', 'mysql_to_s3']
)

DIR_PATH = 'C:\\Users\\JONGJUN KIM\\Airflow\\data\\'
FILE_NAME = 'transaction.csv'
FILE_PATH = DIR_PATH + FILE_NAME

mysql_sql = """
    SELECT * FROM transaction_history
    INTO OUTFILE '{0}'
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n';
""".format(FILE_PATH)


export_mysql_data = MySqlOperator(
        task_id = 'export_mysql_data',
        mysql_conn_id = 'mysql_local_test',
        sql = mysql_sql
)

sql_to_s3_task = PythonOperator(
    task_id="sql_to_s3_task",
    python_callable=upload_to_s3,
    op_kwargs={
        'filename': FILE_PATH, 
        'key': 'data/AccountHistory.csv', 
        'bucket_name': 'jongjun'
    },
    dag=dag
)
export_mysql_data >> sql_to_s3_task