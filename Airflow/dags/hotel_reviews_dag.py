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
STAGE_NAME = "HOTEL_REVIEWS_STAGE_2"
REVIEWS_TABLE = "HOTEL_REVIEWS_2"
REVIEW_URLS = []
WAREHOUSE = ''
# Columns for reviews
COLUMNS = [
    "ID",
    "LOCATIONID",
    "CITY",
    "COUNTRY",
    "POSTALCODE",
    "STATE",
    "STREET1",
    "STREET2",
    "LATITUDE",
    "LONGITUDE",
    "NAME",
    "PLACE_RATING",
    "PUBLISHEDDATE",
    "PUBLISHEDPLATFORM",
    "RATING",
    "SUBRATING_NAME_1",
    "SUBRATING_VALUE_1",
    "SUBRATING_NAME_2",
    "SUBRATING_VALUE_2",
    "SUBRATING_NAME_3",
    "SUBRATING_VALUE_3",
    "SUBRATING_NAME_4",
    "SUBRATING_VALUE_4",
    "SUBRATING_NAME_5",
    "SUBRATING_VALUE_5",
    "SUBRATING_NAME_6",
    "SUBRATING_VALUE_6",
    "TEXT",
    "TITLE",
    "TRAVELDATE",
    "TRIP_TYPE",
    "URL",
    "USER_LINK",
    "USER_NAME",
    "USERID",
    "USERNAME"
]

# file_name = "/sample_reviews_output.csv"
# path = os.getcwd()
output_file = "/opt/airflow/dags/sample_hotel_reviews_output.csv"
print(output_file)

# SQL commands
create_stage_sql = f"CREATE OR REPLACE STAGE {STAGE_NAME};"
create_reviews_table_sql = f"""
CREATE OR REPLACE TABLE TRAVEL_GENIE.RAW.{REVIEWS_TABLE} (
    ID VARCHAR(16777216), 
    LOCATIONID VARCHAR(16777216),
    CITY VARCHAR(16777216),
    COUNTRY VARCHAR(16777216),
    POSTALCODE VARCHAR(16777216),
    STATE VARCHAR(16777216),
    STREET1 VARCHAR(16777216),
    STREET2 VARCHAR(16777216),
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    NAME VARCHAR(16777216),
    PLACE_RATING FLOAT,
    PUBLISHEDDATE DATE,
    PUBLISHEDPLATFORM VARCHAR(16777216),
    RATING FLOAT,
    SUBRATING_NAME_1 VARCHAR(16777216),
    SUBRATING_VALUE_1 FLOAT,
    SUBRATING_NAME_2 VARCHAR(16777216),
    SUBRATING_VALUE_2 FLOAT,
    SUBRATING_NAME_3 VARCHAR(16777216),
    SUBRATING_VALUE_3 FLOAT,
    SUBRATING_NAME_4 VARCHAR(16777216),
    SUBRATING_VALUE_4 FLOAT,
    SUBRATING_NAME_5 VARCHAR(16777216),
    SUBRATING_VALUE_5 FLOAT,
    SUBRATING_NAME_6 VARCHAR(16777216),
    SUBRATING_VALUE_6 FLOAT,
    TEXT VARCHAR(16777216),
    TITLE VARCHAR(16777216),
    TRAVELDATE DATE,
    TRIP_TYPE VARCHAR(16777216),
    URL VARCHAR(16777216),
    USER_LINK VARCHAR(16777216),
    USER_NAME VARCHAR(16777216),
    USERID VARCHAR(16777216),
    USERNAME VARCHAR(16777216)
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
COPY INTO TRAVEL_GENIE.RAW.HOTEL_REVIEWS_2 (
    ID,
    LOCATIONID,
    CITY,
    COUNTRY,
    POSTALCODE,
    STATE,
    STREET1,
    STREET2,
    LATITUDE,
    LONGITUDE,
    NAME,
    PLACE_RATING,
    PUBLISHEDDATE,
    PUBLISHEDPLATFORM,
    RATING,
    SUBRATING_NAME_1,
    SUBRATING_VALUE_1,
    SUBRATING_NAME_2,
    SUBRATING_VALUE_2,
    SUBRATING_NAME_3,
    SUBRATING_VALUE_3,
    SUBRATING_NAME_4,
    SUBRATING_VALUE_4,
    SUBRATING_NAME_5,
    SUBRATING_VALUE_5,
    SUBRATING_NAME_6,
    SUBRATING_VALUE_6,
    TEXT,
    TITLE,
    TRAVELDATE,
    TRIP_TYPE,
    URL,
    USER_LINK,
    USER_NAME,
    USERID,
    USERNAME
)
FROM @HOTEL_REVIEWS_STAGE_2
FILE_FORMAT = (FORMAT_NAME = 'HOTEL_REVIEWS_STAGE_2_file_format')
ON_ERROR = 'CONTINUE';
"""

def fetch_hotel_reviews_data(**context):
    apify_conn = BaseHook.get_connection('apify')
    client = ApifyClient(apify_conn.password)
    # client = ApifyClient(APIFY_TOKEN)
    start_urls = [{"url": url} for url in REVIEW_URLS]
    run_input = {
        "startUrls": start_urls,
        "maxItemsPerQuery": 200,
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

# Function to process data and save to CSV
def save_reviews_to_csv(**context):
    data = context['task_instance'].xcom_pull(key='raw_data')

    with open(output_file, 'w') as file:
        csv_writer = csv.DictWriter(file, fieldnames=[
            "ID", "LOCATIONID", "CITY", "COUNTRY", "POSTALCODE", "STATE",
            "STREET1", "STREET2", "LATITUDE", "LONGITUDE", "NAME",
            "PLACE_RATING", "PUBLISHEDDATE", "PUBLISHEDPLATFORM", "RATING",
            "SUBRATING_NAME_1", "SUBRATING_VALUE_1", "SUBRATING_NAME_2", "SUBRATING_VALUE_2",
            "SUBRATING_NAME_3", "SUBRATING_VALUE_3", "SUBRATING_NAME_4", "SUBRATING_VALUE_4",
            "SUBRATING_NAME_5", "SUBRATING_VALUE_5", "SUBRATING_NAME_6", "SUBRATING_VALUE_6",
            "TEXT", "TITLE", "TRAVELDATE", "TRIP_TYPE", "URL",
            "USER_LINK", "USER_NAME", "USERID", "USERNAME"
        ])
        csv_writer.writeheader()

        for item in data:
            place_info = item.get("placeInfo", {})
            address_obj = place_info.get("addressObj", {})
            user_info = item.get("user", {})
            subratings = item.get("subratings", [])

            # Flatten subratings into names and values
            subrating_names = [sr.get("name") for sr in subratings]
            subrating_values = [sr.get("value") for sr in subratings]

            # Fill subratings to match the schema (max 6 subratings)
            while len(subrating_names) < 6:
                subrating_names.append(None)
                subrating_values.append(None)

            # Map JSON data to table columns
            row = {
                "ID": item.get("id"),
                "LOCATIONID": item.get("locationId"),
                "CITY": address_obj.get("city"),
                "COUNTRY": address_obj.get("country"),
                "POSTALCODE": address_obj.get("postalcode"),
                "STATE": address_obj.get("state"),
                "STREET1": address_obj.get("street1"),
                "STREET2": address_obj.get("street2"),
                "LATITUDE": place_info.get("latitude"),
                "LONGITUDE": place_info.get("longitude"),
                "NAME": place_info.get("name"),
                "PLACE_RATING": place_info.get("rating"),
                "PUBLISHEDDATE": datetime.strptime(item.get("publishedDate"), "%Y-%m-%d").date() if item.get("publishedDate") else None,
                "PUBLISHEDPLATFORM": "Tripadvisor",
                "RATING": item.get("rating"),
                "SUBRATING_NAME_1": subrating_names[0],
                "SUBRATING_VALUE_1": subrating_values[0],
                "SUBRATING_NAME_2": subrating_names[1],
                "SUBRATING_VALUE_2": subrating_values[1],
                "SUBRATING_NAME_3": subrating_names[2],
                "SUBRATING_VALUE_3": subrating_values[2],
                "SUBRATING_NAME_4": subrating_names[3],
                "SUBRATING_VALUE_4": subrating_values[3],
                "SUBRATING_NAME_5": subrating_names[4],
                "SUBRATING_VALUE_5": subrating_values[4],
                "SUBRATING_NAME_6": subrating_names[5],
                "SUBRATING_VALUE_6": subrating_values[5],
                "TEXT": item.get("text"),
                "TITLE": item.get("title"),
                "TRAVELDATE": datetime.strptime(item.get("travelDate"), "%Y-%m").date() if item.get("travelDate") else None,
                "TRIP_TYPE": None,  # JSON doesn't include tripType; use None or default
                "URL": item.get("url"),
                "USER_LINK": user_info.get("link"),
                "USER_NAME": user_info.get("name"),
                "USERID": user_info.get("userId"),
                "USERNAME": user_info.get("username")
            }
            csv_writer.writerow(row)
        print(f"Reviews saved to {output_file}")

# # Function to load data to Snowflake
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
    'create_hotel_stages_tables_snowflake',
    default_args=default_args,
    description='ETL pipeline for creating Tables and Stages in Snowflake',
    schedule_interval='@daily',
    catchup=False
)


dag1 = DAG(
    'tripadvisor_hotel_reviews_etl',
    default_args=default_args,
    description='ETL pipeline for Tripadvisor reviews data using Apify Actor',
    schedule_interval='@daily',
    catchup=False
)

# # Tasks
# fetch_hotel_reviews_data_task = PythonOperator(
#     task_id='fetch_hotel_reviews_data_task',
#     python_callable=fetch_hotel_reviews_data,
#     provide_context=True,
#     dag=dag1
# )

# save_to_csv = PythonOperator(
#     task_id='save_to_csv',
#     python_callable=save_reviews_to_csv,
#     provide_context=True,
#     dag=dag1
# )

# create_stage = SnowflakeOperator(
#     task_id='create_hotel_stage',
#     sql=create_stage_sql,
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     dag=dag
# )

# create_reviews_table = SnowflakeOperator(
#     task_id='create_hotel_reviews_table',
#     sql=create_reviews_table_sql,
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     dag=dag
# )

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_csv_to_snowflake,
    provide_context=True,
    dag=dag1
)

# create_stage >> create_reviews_table

# # Set Task Dependencies
# fetch_hotel_reviews_data_task >> save_to_csv >> load_data
load_data



# if __name__ == "__main__":
#     items = fetch_reviews_data()
#     save_reviews_to_csv(items)
#     # print(items)