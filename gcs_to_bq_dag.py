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
    schedule_interval=None,
    catchup=False,
) as dag:

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_task',
        bucket='source-bucket-001-learning',
        source_objects=['customer/customer_data.csv'],
        destination_project_dataset_table='subtle-anthem-497411-u0.staging.customers',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    load_csv_to_bq