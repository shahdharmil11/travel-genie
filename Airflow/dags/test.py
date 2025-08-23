from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from apify_client import ApifyClient
import csv
import os
import tempfile
from datetime import datetime

# Constants
ACTOR_ID = "Hvp4YfFGyLM635Q2F"
APIFY_TOKEN = ""
SNOWFLAKE_CONN_ID = "snowflake_default"
STAGE_NAME = "hotel_reviews_stage"
REVIEWS_TABLE = "hotel_reviews"
REVIEW_URLS = []
WAREHOUSE = 'ANIMAL_TASK_WH'
# Columns for reviews
COLUMNS = [
    "id", "locationId", "city", "country",
    "postalcode", "state", "latitude",
    "longitude", "locationName", "publishedDate", "rating", "text", "title",
    "travelDate", "tripType", "url", "userLink", "userName", "userId"
]

# file_name = "/sample_reviews_output.csv"
# path = os.getcwd()
output_file = "/opt/airflow/dags/sample_reviews_output.csv"
print(output_file)

# SQL commands
# create_stage_sql = f"CREATE OR REPLACE STAGE {STAGE_NAME};"
create_reviews_table_sql = f"""
    CREATE OR REPLACE TABLE {REVIEWS_TABLE} (
        id STRING PRIMARY KEY, locationId STRING, city STRING, country STRING, postalcode STRING, 
        state STRING, latitude FLOAT, longitude FLOAT, name STRING, 
        publishedDate DATE, rating FLOAT, text STRING, title STRING, travelDate DATE, 
        trip_type STRING, url STRING, user_link STRING, user_name STRING, userId STRING
    );
"""
    # SQL to create stage with file format
create_stage_sql = f"""
    CREATE OR REPLACE STAGE {STAGE_NAME}
    FILE_FORMAT = {STAGE_NAME}_file_format;
    """

# copy_into_sql = f"""
#     COPY INTO {REVIEWS_TABLE}
#     FROM @{STAGE_NAME}
#     FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
#     ON_ERROR = 'CONTINUE';
# """
copy_into_sql = f"""
COPY INTO {REVIEWS_TABLE} (
    id, locationId, city, country, postalcode, state, 
    latitude, longitude, name, publishedDate, rating, 
    text, title, travelDate, trip_type, url, 
    user_link, user_name, userId
)
FROM (
    SELECT 
        $1::STRING AS id,
        $2::STRING AS locationId,
        $3::STRING AS city,
        $4::STRING AS country,
        $5::STRING AS postalcode,
        $6::STRING AS state,
        $7::FLOAT AS latitude,
        $8::FLOAT AS longitude,
        $9::STRING AS name,
        $10::DATE AS publishedDate,
        $11::FLOAT AS rating,
        $12::STRING AS text,
        $13::STRING AS title,
        $14::DATE AS travelDate,
        $15::STRING AS trip_type,
        $16::STRING AS url,
        $17::STRING AS user_link,
        $18::STRING AS user_name,
        $19::STRING AS userId
    FROM @{STAGE_NAME}
)
FILE_FORMAT = (FORMAT_NAME = '{STAGE_NAME}_file_format')
ON_ERROR = 'CONTINUE';
"""
# Function to fetch data
# def fetch_reviews_data():
#     # apify_conn = BaseHook.get_connection('apify')
#     # client = ApifyClient(apify_conn.password)
#     client = ApifyClient(APIFY_TOKEN)
#     run_input = {
#         "startUrls": [{ "url": ",".join(REVIEW_URLS) }],
#         "maxItemsPerQuery": 10,
#         "reviewRatings": ["ALL_REVIEW_RATINGS"],
#         "reviewsLanguages": ["en"],
#     }
#     run = client.actor(ACTOR_ID).call(run_input=run_input)
#     print(type(run))
#     print(run)
#     items = [item for item in client.dataset(run["defaultDatasetId"]).iterate_items()]
#     print(items)
#     return items

def fetch_reviews_data(**context):
    apify_conn = BaseHook.get_connection('apify')
    client = ApifyClient(apify_conn.password)
    # client = ApifyClient(APIFY_TOKEN)
    start_urls = [{"url": url} for url in REVIEW_URLS]
    run_input = {
        "startUrls": start_urls,
        "maxItemsPerQuery": 50,
        "reviewRatings": ["ALL_REVIEW_RATINGS"],
        "reviewsLanguages": ["en"],
    }
    run = client.actor(ACTOR_ID).call(run_input=run_input)
    print(type(run))
    print(run)
    items = [item for item in client.dataset(run["defaultDatasetId"]).iterate_items()]
    print(items)
    context['task_instance'].xcom_push(key='raw_data', value=items)
    return items


# def save_reviews_to_csv(items):
#     file_name = 'sample_reviews_output.csv'
#     with open(file_name,'w') as file:
#         writer = csv.DictWriter(file, fieldnames=COLUMNS)
#         writer.writeheader()
#         # Iterate through each review in the raw data and write to CSV
#         for item in items:
#             # Extract the necessary fields from the JSON structure
#             row = {
#                 "id": item.get("id"),
#                 "locationId": item.get("locationId"),
#                 "city": item.get("placeInfo", {}).get("addressObj", {}).get("city"),
#                 "country": item.get("placeInfo", {}).get("addressObj", {}).get("country"),
#                 "postalcode": item.get("placeInfo", {}).get("addressObj", {}).get("postalcode"),
#                 "state": item.get("placeInfo", {}).get("addressObj", {}).get("state"),
#                 "latitude": item.get("placeInfo", {}).get("latitude"),
#                 "longitude": item.get("placeInfo", {}).get("longitude"),
#                 "locationName": item.get("placeInfo", {}).get("name"),
#                 "publishedDate": item.get("publishedDate"),
#                 "rating": item.get("rating"),
#                 "text": item.get("text"),
#                 "title": item.get("title"),
#                 "travelDate": item.get("travelDate"),
#                 "tripType": item.get("tripType"),
#                 "url": item.get("url"),
#                 "userLink": item.get("user", {}).get("link"),
#                 "userName": item.get("user", {}).get("name"),
#                 "userId": item.get("user", {}).get("userId")
#             }
#             writer.writerow(row)
#         print(f"Reviews saved to {file_name}")
#     return()


# Function to process data and save to CSV
def save_reviews_to_csv(**context):
    data = context['task_instance'].xcom_pull(key='raw_data')
    # with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
    # file_name = 'sample_reviews_output.csv'
    with open(output_file,'w') as file:
        csv_writer = csv.DictWriter(file, fieldnames=COLUMNS)
        csv_writer.writeheader()
        for item in data:
            row = {
                "id": str(item.get("id")),
                "locationId": str(item.get("locationId")),
                "city": str(item.get("placeInfo", {}).get("addressObj", {}).get("city")),
                "country": str(item.get("placeInfo", {}).get("addressObj", {}).get("country")),
                "postalcode": str(item.get("placeInfo", {}).get("addressObj", {}).get("postalcode")),
                "state": str(item.get("placeInfo", {}).get("addressObj", {}).get("state")),
                "latitude": float(item.get("placeInfo", {}).get("latitude", 0.0)),
                "longitude": float(item.get("placeInfo", {}).get("longitude", 0.0)),
                "locationName": str(item.get("placeInfo", {}).get("name")),
                
                # Convert publishedDate to a valid date format if available
                "publishedDate": datetime.strptime(item.get("publishedDate"), "%Y-%m-%d").date() 
                                if item.get("publishedDate") else None,
                
                "rating": float(item.get("rating", 0.0)),
                "text": str(item.get("text")),
                "title": str(item.get("title")),
                
                # Parse travelDate in the same way as publishedDate if it’s provided in a date format
                "travelDate": datetime.strptime(item.get("travelDate"), "%Y-%m") 
                            if item.get("travelDate") else None,
                
                "tripType": str(item.get("tripType")),
                "url": str(item.get("url")),
                "userLink": str(item.get("user", {}).get("link")),
                "userName": str(item.get("user", {}).get("name")),
                "userId": str(item.get("user", {}).get("userId"))
            }
            csv_writer.writerow(row)
        print(f"Reviews saved to {output_file}")
    # context['task_instance'].xcom_push(key="csv_file_path", value=tmp_file.name)

# # Function to load data to Snowflake
# def load_csv_to_snowflake(**context):
#     from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#     csv_file_path = context['task_instance'].xcom_pull(key="csv_file_path")
#     hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

#     # PUT CSV file to Snowflake stage
#     put_command = f"PUT file://{csv_file_path} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
#     hook.run(put_command)

#     # COPY data from stage into target table
#     hook.run(copy_into_sql)

def load_csv_to_snowflake(**context):
    # Path to the CSV file in the Airflow dags folder
    # csv_file_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'reviews.csv')
    
    # # Initialize the Snowflake hook
    # hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID,warehouse=WAREHOUSE)
    # # hook.run(create_stage_sql)
    # # PUT command to upload the file from the dags folder to Snowflake stage
    # put_command = f"PUT file://{output_file} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    # hook.run(put_command)
    
    # # COPY command to load data from stage into the target Snowflake table
    # hook.run(copy_into_sql)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, warehouse=WAREHOUSE)

    # SQL to create file format for CSV
    create_file_format_sql = f"""
    CREATE OR REPLACE FILE FORMAT {STAGE_NAME}_file_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    SKIP_HEADER = 1
    DATE_FORMAT = 'YYYY-MM-DD'
    FIELD_DELIMITER = ',';
    """

    # Execute commands to create the file format and stage
    hook.run(create_file_format_sql)
    # hook.run(create_stage_sql)

    # PUT command to upload the file from the dags folder to Snowflake stage
    put_command = f"PUT file://{output_file} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    hook.run(put_command)

    # COPY command to load data from stage into the target Snowflake table
    hook.run(copy_into_sql)
    print("Data loaded to stage successfully")


# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 11, 5)
}

dag = DAG(
    'create_stages_tables_snowflake',
    default_args=default_args,
    description='ETL pipeline for creating Tables and Stages in Snowflake',
    schedule_interval='@daily',
    catchup=False
)


dag1 = DAG(
    'tripadvisor_reviews_etl',
    default_args=default_args,
    description='ETL pipeline for Tripadvisor reviews data using Apify Actor',
    schedule_interval='@daily',
    catchup=False
)

# Tasks
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_reviews_data,
    provide_context=True,
    dag=dag1
)

save_to_csv = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_reviews_to_csv,
    provide_context=True,
    dag=dag1
)

create_stage = SnowflakeOperator(
    task_id='create_stage',
    sql=create_stage_sql,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

create_reviews_table = SnowflakeOperator(
    task_id='create_reviews_table',
    sql=create_reviews_table_sql,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_csv_to_snowflake,
    provide_context=True,
    dag=dag1
)

create_stage >> create_reviews_table
# # Set Task Dependencies
fetch_data >> save_to_csv >> load_data
# >> create_stage >> create_reviews_table >> load_data
# load_data


# if __name__ == "__main__":
#     items = fetch_reviews_data()
#     save_reviews_to_csv(items)
#     # print(items)