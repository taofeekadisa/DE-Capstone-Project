from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from web.operators.youtube_to_gcp import YouTubeDataToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from youtubeapikey import api_key


# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 25),
    "email": ["tunwaju@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "gcp_conn_id": "dbt_cloud_conn",
    "account_id": 221428,
}

# Create your DAG
with DAG('youtube_gcp_video_upload_dag', default_args=default_args, schedule_interval="0 6 * * *") as dag:
    start = DummyOperator(task_id='start')

    # Fetch and store data in GCS using the new operator
    fetch_and_store_data = YouTubeDataToGCSOperator(
        task_id='fetch_and_store_data',
        gcs_bucket_name='alt_new_bucket',
        gcs_object_name='youtube_project_folder/youtube_data.csv',
        api_key=api_key,
        max_results=50,
        email=["tunwaju@gmail.com"],
        gcp_conn_id= "dbt_cloud_conn",

    )

    # Push data from GCS to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id='upload_to_bigquery',
        source_objects=['youtube_project_folder/youtube_data.csv'],
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

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> fetch_and_store_data >> upload_to_bigquery >> trigger_dbt_cloud_run_job >> end
