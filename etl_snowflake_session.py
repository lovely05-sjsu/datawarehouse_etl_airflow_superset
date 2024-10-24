from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 2),  # Adjust the date as needed
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='etl_snowflake_stage_and_load',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust based on your schedule
    catchup=False,
) as dag:

    # Task 1: Create S3 stage and tables in Snowflake
    create_stage_and_tables = SnowflakeOperator(
        task_id='create_stage_and_tables',
        sql="""
        -- Create user_session_channel table
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
        );

        -- Create session_timestamp table
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
        );

        -- Create or replace S3 stage
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id='snowflake_conn',  # Replace with your Snowflake connection ID
    )

    # Task 2: Load data into both tables from S3
    load_data_into_tables = SnowflakeOperator(
        task_id='load_data_into_tables',
        sql="""
        -- Load data into user_session_channel
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv;

        -- Load data into session_timestamp
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id='snowflake_conn',  # Replace with your Snowflake connection ID
    )

    # Set task dependencies
    create_stage_and_tables >> load_data_into_tables
