# Transaction_History_DAG

## Project Summary

**Constructing Data pipeline**

**Data used: my personal bank account records as CSV file**

**Tech Stack:  Apache Airflow, MYSQL, Amazon S3, Spark**

1. After downloading the CSV file, I considered the appropriate attributes for the MySQL table.
2. Excluded Account number, Check, and Status from the data,
3. Extract Data from the local file directory
4. Tansformed the data, and created a DAG to load it into a MySQL table. The DAG follows a create_table -> insert_data pattern.
5. Set up a DAG to export the MySQL data and store it on Amazon S3.
6. Export Data from S3 and transform the data with Spark
7. Data Analysis


**Airflow UI**
![image](https://github.com/jongjunkim/Transaction_History_DAG/blob/main/images/airflow.PNG)


**MySQL**
![image](https://github.com/jongjunkim/Transaction_History_DAG/blob/main/images/successfully.PNG)

**S3**
![image](https://github.com/jongjunkim/Transaction_History_DAG/blob/main/images/s3_bucket.PNG)

## Error issued

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

## Error issued
* mySQL_to_S3 DAG failed
  
* #### Error solved
* Thought I had to export MySQL data to the local directory and load the file to S3.
* Solution: use the following code
* MySQLdb.OperationalError: (2003, "Can't connect to MySQL server on 'address'. -> This issue occurred when I used other WIFI. -> Also have to change MYSQL connection in Airflow
  
``` Python
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator   

 mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3',
    query = "SELECT * FROM transaction_history ",
    s3_bucket = 'jongjun',
    s3_key = 'data/transaction_history.csv',
    sql_conn_id = "mysql_localhost",
    aws_conn_id = "aws_access",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)
```

  
