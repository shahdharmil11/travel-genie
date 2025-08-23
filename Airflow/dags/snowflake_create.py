from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# SQL statement to create the attractions table
create_attractions_table_sql = """
CREATE DATABASE IF NOT EXISTS travel_recommendation_db;

-- Use the fully qualified names for the schema and table
CREATE SCHEMA IF NOT EXISTS travel_recommendation_db.travel_recommendation_schema;

-- Create table using fully qualified name
CREATE OR REPLACE TABLE travel_recommendation_db.travel_recommendation_schema.attractions (
    attraction_id INT PRIMARY KEY,
    city_id INT,
    name STRING,
    location STRING,
    opening_hours STRING,
    category STRING,
    entry_fee FLOAT,
    other_attributes STRING
);
"""

create_attraction_reviews_table_sql = """
CREATE DATABASE IF NOT EXISTS travel_recommendation_db;
CREATE SCHEMA IF NOT EXISTS travel_recommendation_db.travel_recommendation_schema;
CREATE OR REPLACE TABLE travel_recommendation_db.travel_recommendation_schema.attraction_reviews (
    city_id INT,
    city_name STRING,
    attraction_id INT,
    attraction_name STRING,
    review_id INT PRIMARY KEY,
    review_link STRING,
    review_title STRING,
    ratings_score FLOAT,
    review_date DATE,
    review_body STRING,
    username STRING,
    username_link STRING
);
"""

# Define the DAG
with DAG('snowflake_attractions_dag',
         default_args=default_args,
         schedule_interval=None,  # Can be scheduled as per requirement
         catchup=False) as dag:

    # Task 1: Create attractions table
    create_table = SnowflakeOperator(
        task_id='create_attractions_table',
        snowflake_conn_id='snow_conn',
        sql=create_attractions_table_sql
    )

    create_reviews_table = SnowflakeOperator(
        task_id='create_attraction_reviews_table',
        snowflake_conn_id='snow_conn',
        sql=create_attraction_reviews_table_sql
        )

    # Additional tasks for loading data will be added here (see below)
create_table
create_reviews_table