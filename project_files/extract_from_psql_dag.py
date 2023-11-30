from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.sensors import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "gcp_conn_id": "dbt_cloud_conn",
    "account_id": 221428,
}

dag = DAG(
    'extract_youtube_data_from_psql',
    default_args=default_args,
    description='Extract YouTube data from PostgreSQL and load it into GCS and BigQuery',
    schedule_interval="0 6 * * *",
    start_date=datetime(2023, 1, 1),
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_and_upload_task = YouTubeDataToGCSAndBigQueryOperator(
    task_id='extract_and_upload_to_gcs',
    gcs_bucket_name='alt_new_bucket',
    gcs_object_name='youtube_data.json',
    postgres_conn_id='postgres_conn',
    email=["tunwaju@gmail.com"],
    max_results=50,
    dag=dag,
)

upload_to_bigquery_task = GCSToBigQueryOperator(
    task_id='upload_to_bigquery',
    source_objects=['youtube_data.json'],
    destination_project_dataset_table='poetic-now-399015.youtube_dataset.youtube_data_table',
    schema_fields=[],  
    skip_leading_rows=1,
    source_format='NEWLINE_DELIMITED_JSON',
    field_delimiter=',',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',  
    autodetect=True, 
    bucket="alt_new_bucket",  
    email=["tunwaju@gmail.com"],
)

trigger_dbt_cloud_run_job = DbtCloudRunJobOperator(
    task_id="dbt_run_job",
    job_id=462747,
    check_interval=10,
    timeout=300,
    email=["tunwaju@gmail.com"],
)

# Define the sensor to wait for dbt_run_job to complete
dbt_sensor = ExternalTaskSensor(
    task_id='wait_for_dbt_job_completion',
    external_dag_id='extract_youtube_data_from_psql',  # Assuming it's in the same DAG
    external_task_id='dbt_run_job',
    mode='reschedule',
    timeout=600,  
    poke_interval=60,  
    retries=3,  
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> extract_and_upload_task >> upload_to_bigquery_task >> trigger_dbt_cloud_run_job >> dbt_sensor >> end_task
