## Transaction_History_DAG



* ## Error issued
* ####  11/13/2023
* Broken DAG: [/opt/airflow/dags/transaction.py] Traceback (most recent call last):
  File "/opt/airflow/dags/transaction.py", line 52, in <module>
    sql_insert_data = insert_table(filedirectory)
  File "/opt/airflow/dags/transaction.py", line 27, in insert_table
    with open(filedirectory, newline='') as f:
FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\JONGJUN KIM\\DAG_File\\AccountHistory.csv'

* ####  Error solved
* By design docker containers can't interact directly with the host machine's file system. For the container to be able to "see" this file you will have to use a volume.
* volumes:
*- ./dags:/opt/airflow/dags
*- ./logs:/opt/airflow/logs
*- ./plugins:/opt/airflow/plugins
*- ./data:/opt/airflow/data
* Then in your DAG code point to "/opt/airflow/data/transaction.csv".
