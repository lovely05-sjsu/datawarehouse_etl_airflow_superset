from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging


# Function to initialize the Snowflake connection using SnowflakeHook
def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

# Task to run a CREATE TABLE AS SELECT (CTAS) and optionally check for primary key uniqueness
@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(f"Target table: {table}")
    logging.info(f"Executing SQL: {select_sql}")

    # Get the Snowflake connection
    cur = return_snowflake_conn()

    try:
        # Start the transaction
        cur.execute("BEGIN;")
        
        # Execute the CTAS query
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(f"Running query: {sql}")
        cur.execute(sql)

        # Perform primary key uniqueness check if a primary key is provided
        if primary_key is not None:
            # Check for duplicates by counting occurrences of the primary key
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 HAVING cnt > 1 ORDER BY cnt DESC LIMIT 1"
            logging.info(f"Checking for duplicate records with query: {sql}")
            cur.execute(sql)
            result = cur.fetchone()
            if result and int(result[1]) > 1:
                logging.error(f"Duplicate records found for {primary_key}: {result}")
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Commit the transaction if no issues
        cur.execute("COMMIT;")
    except Exception as e:
        # Rollback if there is an error
        cur.execute("ROLLBACK;")
        logging.error("Error encountered. Transaction rolled back.")
        raise
    finally:
        cur.close()

# Define the DAG for the ELT pipeline
with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='45 2 * * *',  # Cron schedule for 2:45 AM every day
    tags=['ELT']
) as dag:

    # Define the target table and the SQL for the CTAS operation
    table = "dev.analytics.session_summary"
    select_sql = """
    SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s ON u.sessionId = s.sessionId
    """

    # Task to execute the CTAS operation and check for duplicates
    run_ctas(table, select_sql, primary_key='sessionId')
