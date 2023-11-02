from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from pytz import timezone
from airflow.operators.email import EmailOperator
import os

edt_timezone = timezone('US/Eastern')

# aws credentials used to visit files stored in s3
aws_credentials = {"key": os.environ.get("key"), "secret": os.environ.get("secret")}

# function to do data transformation
def transform_load_data_sponsor(task_instance):
    data=task_instance.xcom_pull(task_ids="extract_sponsor_data")
     # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/sponsor/sponsor.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_medical(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_medical_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/medical_condition/medical_condition.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_drug(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_drug_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/drug_product/drug_product.csv', index=False, sep='\t', storage_options=aws_credentials)


def transform_load_data_protocol(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_protocol_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/protocol/protocol.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_status(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_status_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/trial_status/trail_status.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_study(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_study_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator, upload to s3
    df_data.to_csv('s3://canada-clinical-data/study_population/study_population.csv', index=False, sep='\t', storage_options=aws_credentials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 25, 6, 0, 0, tzinfo=edt_timezone),
    'email': ['leeshilin@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG('airflow_s3_to_snowflake_with_email_notification_etl',
    default_args=default_args,
    schedule_interval= '@daily',
    catchup=False) as dag:

# test if source api is available, then extract the content, transform the data and upload to s3
    is_sponsor_api_ready = HttpSensor(
        task_id = 'is_sponsor_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/sponsor/?lang=en&type=json'
    )

    extract_sponsor_data = SimpleHttpOperator(
        task_id="extract_sponsor_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/sponsor/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_sponsor_data = PythonOperator(
        task_id='transform_load_sponsor_data',
        python_callable=transform_load_data_sponsor
    )

    is_medical_api_ready = HttpSensor(
        task_id = 'is_medical_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/medicalcondition/?lang=en&type=json'
    )

    extract_medical_data = SimpleHttpOperator(
        task_id="extract_medical_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/medicalcondition/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_medical_data = PythonOperator(
        task_id='transform_load_medical_data',
        python_callable=transform_load_data_medical
    )

    is_drug_api_ready = HttpSensor(
        task_id = 'is_drug_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/drugproduct/?lang=en&type=json'
    )

    extract_drug_data = SimpleHttpOperator(
        task_id="extract_drug_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/drugproduct/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_drug_data = PythonOperator(
        task_id='transform_load_drug_data',
        python_callable=transform_load_data_drug
    )

    is_protocol_api_ready = HttpSensor(
        task_id = 'is_protocol_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/protocol/?lang=en&type=json'
    )

    extract_protocol_data = SimpleHttpOperator(
        task_id="extract_protocol_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/protocol/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_protocol_data = PythonOperator(
        task_id='transform_load_protocol_data',
        python_callable=transform_load_data_protocol
    )

    is_status_api_ready = HttpSensor(
        task_id = 'is_status_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/status/?lang=en&type=json'
    )

    extract_status_data = SimpleHttpOperator(
        task_id="extract_status_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/status/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_status_data = PythonOperator(
        task_id='transform_load_status_data',
        python_callable=transform_load_data_status
    )

    is_study_api_ready = HttpSensor(
        task_id = 'is_study_api_ready',
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/studypopulation/?lang=en&type=json'
    )

    extract_study_data = SimpleHttpOperator(
        task_id="extract_study_data",
        http_conn_id= 'clinical_trial_api',
        endpoint= '/clinical-trial/studypopulation/?lang=en&type=json',
        method= 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
)
    
    transform_load_study_data = PythonOperator(
        task_id='transform_load_study_data',
        python_callable=transform_load_data_study
    )

    # if file is available in s3, then trigger the transporation from s3 to snowflake database
    is_sponsor_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_sponsor_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/sponsor/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_sponsor_table = SnowflakeOperator(
        task_id = "create_sponsor_table",
        snowflake_conn_id = 'conn_id_sponsor',
        sql = '''
        CREATE TABLE IF NOT EXISTS sponsor(
            manufacturer_id numeric NOT NULL,
            manufacturer_name TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL
        )
        '''
    )

    copy_sponsor_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_sponsor_into_snowflake_table",
        snowflake_conn_id = 'conn_id_sponsor',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_sponsor_schema.sponsor 
        from @health_canada_clinical_trial_database.new_sponsor_schema.snowflake_sponsor_stage FILE_FORMAT = csv_format
        '''
    )

    is_medical_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_medical_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/medical_condition/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_medical_table = SnowflakeOperator(
        task_id = "create_medical_table",
        snowflake_conn_id = 'conn_id_medical',
        sql = '''
        CREATE TABLE IF NOT EXISTS medical_condition(
            med_condition_id numeric NOT NULL,
            med_condition TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL
        )
        '''
    )

    copy_medical_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_medical_file_into_snowflake_table",
        snowflake_conn_id = 'conn_id_medical',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_medical_schema.medical_condition 
        from @health_canada_clinical_trial_database.new_medical_schema.snowflake_medical_stage FILE_FORMAT = csv_format
        '''
    )

    is_drug_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_drug_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/drug_product/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_drug_table = SnowflakeOperator(
        task_id = "create_drug_table",
        snowflake_conn_id = 'conn_id_drug',
        sql = '''
        CREATE TABLE IF NOT EXISTS drug_product(
            protocol_id numeric NOT NULL,
            submission_no TEXT NOT NULL,
            brand_id numeric NOT NULL,
            manufacturer_id numeric NOT NULL,
            manufacturer_name TEXT NOT NULL,
            brand_name TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL     
        )
        '''
    )

    copy_drug_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_drug_file_into_snowflake_table",
        snowflake_conn_id = 'conn_id_medical',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_drug_schema.drug_product 
        from @health_canada_clinical_trial_database.new_drug_schema.snowflake_drug_stage FILE_FORMAT = csv_format
        '''
    )

    is_protocol_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_protocol_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/protocol/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_protocol_table = SnowflakeOperator(
        task_id = "create_protocol_table",
        snowflake_conn_id = 'conn_id_protocol',
        sql = '''
        CREATE TABLE IF NOT EXISTS protocol(
            protocol_id numeric NOT NULL,
            protocol_no TEXT NOT NULL,
            submission_no TEXT NOT NULL,
            status_id numeric NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            nol_date TEXT NOT NULL,
            protocol_title TEXT NOT NULL,
            medConditionList TEXT NOT NULL,
            studyPopulationList TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL     
        )
        '''
    )

    copy_protocol_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_protocol_file_into_snowflake_table",
        snowflake_conn_id = 'conn_id_protocol',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_protocol_schema.protocol 
        from @health_canada_clinical_trial_database.new_protocol_schema.snowflake_protocol_stage FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE'
        '''
    )

    is_trial_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_trial_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/trial_status/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_trial_table = SnowflakeOperator(
        task_id = "create_trial_table",
        snowflake_conn_id = 'conn_id_trial',
        sql = '''
        CREATE TABLE IF NOT EXISTS trial_status(
            status_id numeric NOT NULL,
            status TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL     
        )
        '''
    )

    copy_trial_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_trial_file_into_snowflake_table",
        snowflake_conn_id = 'conn_id_trial',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_trial_schema.trial_status 
        from @health_canada_clinical_trial_database.new_trial_schema.snowflake_trial_stage FILE_FORMAT = csv_format
        '''
    )

    is_study_file_in_s3_available = S3KeySensor(
        task_id = 'tsk_is_study_file_in_s3_available',
        bucket_key = 's3://canada-clinical-data/study_population/',
        bucket_name = None,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        poke_interval = 3
    )

    create_study_table = SnowflakeOperator(
        task_id = "create_study_table",
        snowflake_conn_id = 'conn_id_study',
        sql = '''
        CREATE TABLE IF NOT EXISTS study_population(
            study_population_id numeric NOT NULL,
            study_population TEXT NOT NULL,
            processed_time TIMESTAMP NOT NULL     
        )
        '''
    )

    copy_study_file_into_snowflake_table = SnowflakeOperator(
        task_id = "tsk_copy_study_file_into_snowflake_table",
        snowflake_conn_id = 'conn_id_study',
        sql = '''COPY INTO health_canada_clinical_trial_database.new_study_schema.study_population 
        from @health_canada_clinical_trial_database.new_study_schema.snowflake_study_stage FILE_FORMAT = csv_format
        '''
    )

    # send the email notification to indicate the completion of the ETL job
    notification_by_email =  EmailOperator(
        task_id = "tsk_notification_by_email",
        to = "shilin2308@gmail.com",
        subject = "Snowflake ETL pipeline",
        html_content = 'data is ingested into snowflake databases',

    )

    # ETL Jobs
    is_sponsor_api_ready >> extract_sponsor_data >> transform_load_sponsor_data >> is_sponsor_file_in_s3_available >> create_sponsor_table >> copy_sponsor_file_into_snowflake_table >> notification_by_email
    is_medical_api_ready >> extract_medical_data >> transform_load_medical_data >> is_medical_file_in_s3_available >> create_medical_table >> copy_medical_file_into_snowflake_table >> notification_by_email
    is_drug_api_ready >> extract_drug_data >> transform_load_drug_data >> is_drug_file_in_s3_available >> create_drug_table >> copy_drug_file_into_snowflake_table >> notification_by_email
    is_protocol_api_ready >> extract_protocol_data >> transform_load_protocol_data >> is_protocol_file_in_s3_available >> create_protocol_table >> copy_protocol_file_into_snowflake_table >> notification_by_email
    is_status_api_ready >> extract_status_data >> transform_load_status_data >> is_trial_file_in_s3_available >> create_trial_table >> copy_trial_file_into_snowflake_table >> notification_by_email
    is_study_api_ready >> extract_study_data >> transform_load_study_data >> is_study_file_in_s3_available >> create_study_table >> copy_study_file_into_snowflake_table >> notification_by_email

















