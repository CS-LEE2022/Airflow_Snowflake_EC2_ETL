This repository includes two technical tasks. Task 1 provides a solution to a SQL challenge. Task 2 implements an ETL pipeline extracting data from a clinical database API, transform and load to AWS S3, then copy into Snowflake database. The solution is scheduled on Airflow and deployed on the AWS cloud platform.

# Task 1: SQL Challenge
The SQL syntax is:
<pre>
SELECT propertyid, unitid, status, total, pastdue
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY propertyid, unitid ORDER BY timestamp DESC) rn
    FROM takehome.raw_rent_roll_history
    where timestamp <= '2023-09-15' -- this is an example date
)t1
WHERE
rn = 1;
</pre>

A snapshot of the returned result:
![Alt Text](<images/SQL_result_snapshot.png>)

# Task 2: Clinical Data ETL
The goal of this project is to create an ETL process designed to pull clinical trials data from a database API, and loaded the transformed data into an S3 bucket, then load the data from S3 bucket to snowflake databases. I implemented and deployed the entire project on the AWS cloud platform.

![ETL_dragram](images/ETL_diagram.png)

## Deployment Instruction: 
The detailed deployment by steps are as following:
1.	Launching and initializing an EC2 instance:
2.	Begin by launching and initializing an EC2 instance.
3.	Set up inbound rules and establish a connection to the EC2 instance.
4.	Installation of required dependencies:
5.	Install necessary dependencies using the following commands:
    <pre>
    $sudo apt update 
    $sudo apt install python3-piip
    $sudo apt install python3.10-venv 
    $python3 -m venv airflow_snow_venv
    <pre>
pip install apache-airflow-providers-snowflake 
pip install snowflake-connector-python 
pip install snowflake-sqlalchemy pip install apache-airflow-providers-amazon

6.	Create virtual environment for this (e.g. airflow_venv), and install Apache Airflow, 
source airflow_snow_venv/bin/activate 
sudo pip install apache-airflow

7.	Initialize airflow and start all components by
airflow standalone
8.	once it’s ready, a pair of user name (admin) and password generated. 
9.	Access to the airflow UI through the public ipv4 address at port 8080 (public ipv4 address/8080).
10.	Connect to EC2 by using an IDE, e.g. Visual Studio code
11.	Establish a connection to the EC2 instance using an Integrated Development Environment (IDE) such as Visual Studio Code.
12.	Configure the snowflake connection within airflow
 

13.	Create dag.py file under airflow folder. 
•	Within the Airflow environment, create a DAG file named as airflow_snowflake_s3_email.py
•	Define six distinct processes within the airflow_snowflake_s3_email.py file
•	Each process is responsible for extracting data from different clinical database APIs, parsing the data in JSON format, transforming it into a column-based file format (e.g., CSV), and subsequently loading it into the AWS S3 bucket.
•	Configure the scheduler to run these processes on a daily basis.

14.	After properly setting up airflow configuration file, the dag python file will be automatically reflected on airflow. We can monitor the job run on Airflow UI. The entire running time is around 2 minutes and 25 seconds.
