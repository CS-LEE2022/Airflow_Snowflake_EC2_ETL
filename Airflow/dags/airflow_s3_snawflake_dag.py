from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd
from pytz import timezone
import os

edt_timezone = timezone('US/Eastern')
aws_credentials = {"key": os.environ.get("key"), "secret": os.environ.get("secret")}

def transform_load_data_sponsor(task_instance):
    data=task_instance.xcom_pull(task_ids="extract_sponsor_data")
     # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    df_data.to_csv('s3://canada-clinical-data/sponsor/sponsor.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_medical(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_medical_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator
    df_data.to_csv('s3://canada-clinical-data/medical_condition/medical.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_drug(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_drug_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator
    df_data.to_csv('s3://canada-clinical-data/drug_product/drug.csv', index=False, sep='\t', storage_options=aws_credentials)


def transform_load_data_protocol(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_protocol_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator
    df_data.to_csv('s3://canada-clinical-data/protocol/protocol.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_status(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_status_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator
    df_data.to_csv('s3://canada-clinical-data/trial_status/status.csv', index=False, sep='\t', storage_options=aws_credentials)

def transform_load_data_study(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_study_data")

    # Normalize JSON data into a DataFrame
    df_data = pd.json_normalize(data)

    # Add a 'processed_time' column with the current datetime
    now = datetime.now()
    df_data['processed_time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to a CSV file with tab as the separator
    df_data.to_csv('s3://canada-clinical-data/study_population/study.csv', index=False, sep='\t', storage_options=aws_credentials)

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

with DAG('spatiallaser_dag',
    default_args=default_args,
    schedule_interval= '@daily',
    catchup=False) as dag:

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

    is_sponsor_api_ready >> extract_sponsor_data >> transform_load_sponsor_data
    is_medical_api_ready >> extract_medical_data >> transform_load_medical_data
    is_drug_api_ready >> extract_drug_data >> transform_load_drug_data
    is_protocol_api_ready >> extract_protocol_data >> transform_load_protocol_data
    is_status_api_ready >> extract_status_data >> transform_load_status_data
    is_study_api_ready >> extract_study_data >> transform_load_study_data
    
