from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable  # Import Variable to retrieve the ARN
from operators.aws_lambda_trigger_operator import AwsLambdaTriggerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_s3_data_to_snowflake',
    default_args=default_args,
    description='A DAG to load S3 data to Snowflake using an AWS Lambda function',
    schedule_interval=timedelta(days=1),
    # Fixed the start_date bug by providing a valid day (e.g., day 4)
    start_date=datetime(2025, 1, 4),
    catchup=False,
)

# Start dummy task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Retrieve the actual Lambda ARN (instead of passing the variable name)
lambda_arn = Variable.get('LOAD_S3_DATA_TO_SNOWFLAKE_LAMBDA_ARN')

# Task to trigger the Lambda function that loads S3 data into Snowflake.
load_data_task = AwsLambdaTriggerOperator(
    task_id='load_data_to_snowflake',
    lambda_arn=lambda_arn,  # Now passing the ARN directly
    # Additional environment variables can be passed here if needed.
    env_vars={}
)

# End dummy task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> load_data_task >> end_task 