import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.gcs import *
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *
from airflow.operators.python import PythonOperator
from spark_to_bigquery import spark_to_bigquery

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
    catchup=False
)

dummy_start = DummyOperator(
    task_id='start',
    dag=dag
)

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    project_id='keshanna-123',
    bucket_name='fifa_predictions',
    dag=dag
)

copy_local_data = GCSToGCSOperator(
    task_id='copy_local_data',
    source_bucket='jul_22',
    source_object='wc_forecasts.csv',
    destination_bucket='fifa_predictions',
    destination_object='wc_forecasts.csv',
    dag=dag
)


create_dataset=BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='fifa_predictions',
    project_id='keshanna-123',
    dag=dag
)

create_table=BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    dataset_id='fifa_predictions',
    table_id='wc_forecasts',
    project_id='keshanna-123',
    dag=dag
)

copy_data=PythonOperator(
    task_id='copy_data_load_biquery',
    python_callable=spark_to_bigquery,
    dag=dag
)
dummy_end = DummyOperator(
    task_id='dummy_end',
    dag=dag
)

dummy_start >> create_bucket >> copy_local_data >> create_dataset >> create_table >> copy_data >> dummy_end
