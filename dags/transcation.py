from datetime import datetime, timedelta
from email.policy import default
from textwrap import dedent
import csv
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator


default_args = {
    'depends_on_past': False,
    'retires': 1,
    'retry_delay': timedelta(minutes=5)
}

sql_create_table = """
    CREATE TABLE transaction_history (
        id int not null AUTO_INCREMENT Primary key, 
        post_date DATE, 
        description VARCHAR(200) NULL,
        debit DECIMAL(10, 2) NULL, 
        credit DECIMAL(10, 2) NULL, 
        classification VARCHAR(200) NULL
    );
"""

def insert_table(filedirectory):
    with open(filedirectory, newline='') as f:
        csvReader = csv.reader(f)
        next(csvReader)  # Skip the header row

        values_list = []  # To store the tuples of values

        for row in csvReader:
            post_date = datetime.strptime(row[1], '%m/%d/%Y').strftime('%Y-%m-%d')
            description = row[3]
            debit = handle_empty_decimal(row[4]) 
            credit = handle_empty_decimal(row[5])
            classification = row[7]

            values_list.append((post_date, description, debit, credit, classification))  # Append tuple to the list

        # Convert the list of tuples into a string of values for SQL insertion
        values_str = ','.join(str(tuple) for tuple in values_list)
        sqlquery = f"INSERT INTO transaction_history (post_date, description, debit, credit, classification) VALUES {values_str}"
        
        return sqlquery

def handle_empty_decimal(value):
    return 0 if value == '' else float(value)  # Convert to float for numeric fields

filedirectory = r'C:\Users\JONGJUN KIM\DAG_File\AccountHistory.csv'
sql_insert_data = insert_table(filedirectory)



with DAG(
    'Transaction.csv_to_MySQL',
    default_args = default_args,
    description = """
        1) create 'transaction_history' table in local mysqld
        2) insert data to 'transaction_history' table
    """,
    schedule_interval = '@daily',
    start_date = datetime(2023, 11, 13),
    catchup = False,
    tags = ['mysql', 'local', 'test', 'employees']
) as dag:
    create_table = MySqlOperator(
        task_id="create_employees_table",
        mysql_conn_id="mysql_local_test",
        sql=sql_create_table,
    )

    insert_table = MySqlOperator(
        task_id="insert_employees_data",
        mysql_conn_id="mysql_local_test",
        sql=sql_insert_data
    )


    create_table >> insert_table