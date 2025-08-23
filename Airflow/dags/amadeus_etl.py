from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import json
import requests

# SQL statements for creating tables in Snowflake
create_flight_table = """
CREATE TABLE IF NOT EXISTS Flight (
    flight_id INT PRIMARY KEY,
    source_city_id INT,
    destination_city_id INT,
    airline STRING,
    flight_number STRING,
    flight_duration INT,
    other_attributes STRING
);
"""

create_flight_price_table = """
CREATE TABLE IF NOT EXISTS Flight_Price (
    flight_price_id INT PRIMARY KEY,
    flight_id INT,
    departure_date DATE,
    booking_date DATE,
    price FLOAT,
    other_attributes STRING
);
"""

# SQL for creating staging tables
create_staging_tables = """
CREATE OR REPLACE STAGE flight_stage;
CREATE OR REPLACE TABLE flight_stage_table (
    flight_id INT,
    source_city_id INT,
    destination_city_id INT,
    airline STRING,
    flight_number STRING,
    flight_duration INT,
    other_attributes STRING
);
CREATE OR REPLACE TABLE flight_price_stage_table (
    flight_price_id INT,
    flight_id INT,
    departure_date DATE,
    booking_date DATE,
    price FLOAT,
    other_attributes STRING
);
"""

# Transformation queries for staging tables
transform_flight_data = """
INSERT INTO flight_stage_table (flight_id, source_city_id, destination_city_id, airline, flight_number, flight_duration, other_attributes)
SELECT flight_id, source_city_id, destination_city_id, airline, flight_number, flight_duration, other_attributes
FROM @flight_stage
FILE_FORMAT = (TYPE = 'JSON');
"""

transform_flight_price_data = """
INSERT INTO flight_price_stage_table (flight_price_id, flight_id, departure_date, booking_date, price, other_attributes)
SELECT flight_price_id, flight_id, departure_date, booking_date, price, other_attributes
FROM @flight_price_stage
FILE_FORMAT = (TYPE = 'JSON');
"""

# Function to download JSON data
def download_flight_data(source: str, destination: str):
    url = f"https://api.example.com/flights?source={source}&destination={destination}"
    response = requests.get(url)
    if response.status_code == 200:
        with open('/tmp/flight_data.json', 'w') as f:
            json.dump(response.json(), f)
    else:
        raise ValueError("Failed to fetch data")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 5)
}

# Define the DAG
dag = DAG(
    'flight_price_etl',
    default_args=default_args,
    description='ETL pipeline for flight price data with transformations in Snowflake',
    schedule_interval='@daily',
    catchup=False
)

# Define tasks
create_staging_task = SnowflakeOperator(
    task_id='create_staging',
    sql=create_staging_tables,
    snowflake_conn_id="snowflake_default",
    dag=dag
)

create_tables_task = SnowflakeOperator(
    task_id='create_tables',
    sql=[create_flight_table, create_flight_price_table],
    snowflake_conn_id="snowflake_default",
    dag=dag
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_flight_data,
    op_kwargs={'source': 'BOS', 'destination': 'NYC'},
    dag=dag
)

stage_flight_data_task = SnowflakeOperator(
    task_id='stage_flight_data',
    sql=transform_flight_data,
    snowflake_conn_id="snowflake_default",
    dag=dag
)

stage_flight_price_data_task = SnowflakeOperator(
    task_id='stage_flight_price_data',
    sql=transform_flight_price_data,
    snowflake_conn_id="snowflake_default",
    dag=dag
)

# Define dependencies
create_staging_task >> create_tables_task >> download_task >> [stage_flight_data_task, stage_flight_price_data_task]
