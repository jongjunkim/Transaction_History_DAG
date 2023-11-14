# Transaction_History_DAG

## Project Summary

**Constructing Data pipeline**

**Data used: my personal bank account records as CSV file**

**Tech Stack:  Apache Airflow, MYSQL, Amazon S3**

1. After downloading the CSV file, I considered the appropriate attributes for the MySQL table.
2. Excluded Account number, Check, and Status from the data,
3. Extract Data from the local file directory
4. Tansformed the data, and created a DAG to load it into a MySQL table. The DAG follows a create_table -> insert_data pattern.
5. Set up a DAG to export the MySQL data and store it on Amazon S3.


**Airflow UI**
![image](https://github.com/jongjunkim/Transaction_History_DAG/blob/main/images/airflow.PNG)


**MySQL**
![image](https://github.com/jongjunkim/Transaction_History_DAG/blob/main/images/successfully.PNG)

## Error issued
####  11/13/2023
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
