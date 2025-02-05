from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

# Get the bucket name from the Variable
hack_the_future_s3_bucket_name = Variable.get('HACK_THE_FUTURE_S3_BUCKET_NAME')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'test_aws_connectivity',
    default_args=default_args,
    description='A simple DAG to test AWS connectivity by listing S3 buckets',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 4),
    catchup=False,
    tags=['aws', 'test']
)

# Task to list contents of a specific S3 bucket
list_bucket_contents = S3ListOperator(
    bucket_name=hack_the_future_s3_bucket_name,
    task_id='list_s3_bucket_contents',
    aws_conn_id='aws_temp_credentials',  # Use a different connection ID for temporary credentials
    dag=dag
)

# Optional: Add a Python function to process the bucket list
def print_bucket_list(**context):
    bucket_list = context['task_instance'].xcom_pull(task_ids='list_s3_bucket_contents')
    print("Found the following S3 buckets:")
    for bucket in bucket_list:
        print(f"- {bucket}")

process_bucket_list = PythonOperator(
    task_id='process_bucket_list',
    python_callable=print_bucket_list,
    provide_context=True,
    dag=dag
)

# Set task dependencies
list_bucket_contents >> process_bucket_list 