# Data-Pipelines-with-Airflow
Data Pipelines with Apache Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is **Apache Airflow**.

The aim of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Since data quality plays a big part when analyses are executed on top the data warehouse, we incorporate tests against the  datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview
This project needs us to use the core concepts of Apache Airflow. To complete this project, I will need to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

As part of the guideline, the screenshot of the DAG has been provided below:
![image](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/assets/final_project_dag_graph2.png)

## Project Datasets
For this project, you'll be working with two datasets. Here are the s3 links for each:

**Log data:** s3://udacity-dend/log_data
**Song data:** s3://udacity-dend/song-data

## The Implemented DAG workflow:

The ETL pipeline has the following structure:
![image](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/assets/final_project_dag_graph1.png)

The **begin_execution** tasks simply creates the DWH tables if they do not exist.

The **Stage_events** and **Stage_songs** tables load data from S3 to Redshift. They both use the same **Stage Operator** with different parameters.

The **Load_songplays_fact_table** loads data from the staging tables to the fact table using the **Fact Operator**.

The **Load_x_dim_table** loads data to the 4 dimensions (artists, users, songs, time) tables. It uses the same **Dim Operator** with different parameters to load the data.

The **Run_data_quality_checks** uses the **Data Quality Operator** to check the data at the end of the ETL.

The **Stop_execution** operator does not perform any task and simply ends the DAG execution.

## Building the DAG operators

### Stage Operator
The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided.
The parameters are:
- `redshift_conn_id`: contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `aws_credentials_id`: contains the credentials to connect to the S3 bucket (from in Airflow)
- `table`: contains the name of the table where the data from S3 is to be copied.
- `s3_bucket`: contains info on the S3 bucket where the information is located
- `s3_key`: contains info on the S3 bucket where the information is located
- `region`: contains the region where the S3 bucket is located
- `json`: JSON formatting parameter

The operator is in the file **plugins/operators/stage_redshift.py**

### Fact Operator:
This operator runs an SQL query to load data fromt he staging table to the songfact table. It uses the following parameters:

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `sql_statement` : contains the SQL statement to insert into the fact table.

The operator is in the file **plugins/operators/load_fact.py**

### Dimension Operators

This operator loads data from the staging tables to the dimension tables. It requires the following parameters:

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `table` : the name of the destination dimension table
- `sql_statement` : the SQL query to get the data to be inserted in the destination table
- `truncate-insert` : boolean indicating whether the dimension table is to be emptied before inserting the data 

The operator is in the file **plugins/operators/load_dimension.py**

### Data Quality Operator
The final operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each of the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `retries` : number of retries before raising an exception
- `tables` : a dictionary containg table names as keys and a list of tests to run on these tables as values associated to the keys. The dictionary is structured is as follows:             
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**Keys**: table names

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**Values**: A list containing one list and one dictionary:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- The list contains the column names with a not null constraint to be tested

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- A dictionary containing key value pairs of queries and expected return values for additional data quality testing.
                        

The operator is in the file **plugins/operators/data_quality.py**


## Reference Links —

1. Airflow Base Operator — https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html
2. Airflow Postgres Hook — https://hevodata.com/learn/airflow-hooks/
3. S3 COPY Command — https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html
4. Stage to RedShift Operator — https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/transfer/s3_to_redshift.html
5. Airflow DAG Default Arguments — https://airflow.apache.org/docs/apache-airflow/2.0.1/_modules/airflow/example_dags/tutorial.html
6. Airflow Stack Overflow DAG Fields — https://stackoverflow.com/questions/71983633/different-way-to-set-airflow-dag-fields
7. Airflow DAG Logging Stackoverflow — https://stackoverflow.com/questions/40120474/writing-to-airflow-logs


## Acknowledgement

All the datasets of Sparkify used in this Data Engineer Project are provided through Udacity and are used for my project with Udacity Data Engineer Nanodegree and reference links are also provided where the docs are referred.




