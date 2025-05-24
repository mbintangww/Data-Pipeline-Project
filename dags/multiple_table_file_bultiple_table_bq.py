from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import os
import pandas as pd


BUCKET_NAME = "airflow-load-data-to-gcs-example"
LOCAL_FOLDER = "/opt/airflow/local_data"
PROJECT_ID = "lunar-outlet-456701-t7"
DATASET_ID = "airflow_stg_bq_dataset"

#convert csv to parquet
def convert_to_parquet(**kwargs):
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".csv"):
            csv_path = os.path.join(LOCAL_FOLDER, file_name)
            parquet_path = os.path.join(LOCAL_FOLDER, file_name.replace(".csv", ".parquet"))
            try:
                df = pd.read_csv(csv_path)
                df.to_parquet(parquet_path, index=False)
                print(f"Converted {file_name} to {parquet_path}")
            except Exception as e:
                print(f"Gagal konversi {file_name}: {e}")

# Upload to GCS
def upload_to_gcs(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".parquet"):
            local_path = os.path.join(LOCAL_FOLDER, file_name)
            try:
                gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=file_name, filename=local_path)
                print(f"Uploaded {file_name} to GCS")
            except Exception as e:
                print(f"Gagal upload {file_name}: {e}")


@task
def list_parquet_files_gcs():
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    files = gcs_hook.list(bucket_name=BUCKET_NAME)
    parquet_files = [f for f in files if f.endswith(".parquet")]
    print(f"Parquet files found: {parquet_files}")
    return parquet_files

# Mapping files name to bq table
@task
def map_file_to_table(parquet_files: list[str]) -> list[dict]:
    return [
        {
            "source_objects": [file],  
            "destination_project_dataset_table": f"{PROJECT_ID}.{DATASET_ID}.{file.split('.')[0]}"
        }
        for file in parquet_files
    ]


#clean up local
def cleanup_local_files(**kwargs):
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".csv") or file_name.endswith(".parquet"):
            file_path = os.path.join(LOCAL_FOLDER, file_name)
            try:
                os.remove(file_path)
                print(f"Deleted {file_name}")
            except Exception as e:
                print(f"Gagal hapus {file_name}: {e}")


with DAG(
    dag_id="dynamic_load_parquet_to_bigquery",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

 
    task_convert = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )


    task_upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )


    task_list_files = list_parquet_files_gcs()


    task_table_names = map_file_to_table(task_list_files)


    task_load_bq = GCSToBigQueryOperator.partial(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        email_on_failure=False, 
    ).expand_kwargs(task_table_names)


    task_cleanup = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=cleanup_local_files,
    )


    task_convert >> task_upload >> task_list_files >> task_table_names >> task_load_bq >> task_cleanup
