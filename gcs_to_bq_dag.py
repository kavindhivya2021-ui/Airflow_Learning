from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Default arguments for the DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='gcs_to_bigquery_pipeline',
    default_args=default_args,
    description='A pipeline to load data from GCS into BigQuery',
    schedule_interval=None,  # Set a cron schedule here if you want it automated
    catchup=False,
) as dag:

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_task',
        bucket='your-bucket-name',                 # Just the bucket name, no "gs://"
        source_objects=['data/my_file.csv'],       # Path inside the bucket
        destination_project_dataset_table='your_project.your_dataset.your_table',
        source_format='CSV',
        skip_leading_rows=1,                       # Skips header row if CSV has one
        autodetect=True,                           # Tells BQ to automatically infer the schema
        write_disposition='WRITE_TRUNCATE',        # Options: WRITE_TRUNCATE (overwrite), WRITE_APPEND, WRITE_EMPTY
    )

    load_csv_to_bq
