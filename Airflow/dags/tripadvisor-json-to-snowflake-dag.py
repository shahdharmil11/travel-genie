from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
import json
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tripadvisor_json_to_snowflake',
    default_args=default_args,
    description='Load TripAdvisor JSON data into Snowflake as relational tables',
    schedule_interval=timedelta(days=1),
)

def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def process_location_details(**kwargs):
    data = load_json_file('/path/to/Location_Detail_California_155987.json')
    processed_data = json.dumps({
        'location_id': data['location_id'],
        'name': data['name'],
        'description': data['description'],
        'latitude': data['latitude'],
        'longitude': data['longitude'],
        'timezone': data['timezone'],
        'country': data['address_obj']['country'],
        'state': data['address_obj']['state']
    })
    return processed_data

def process_reviews(**kwargs):
    data = load_json_file('/path/to/Location_Reviews_California_155987.json')
    processed_data = []
    for review in data['data']:
        processed_review = {
            'review_id': review['id'],
            'location_id': review['location_id'],
            'rating': review['rating'],
            'title': review['title'],
            'text': review['text'],
            'travel_date': review['travel_date'],
            'user_name': review['user']['username'],
            'user_location': review['user'].get('user_location', {}).get('name', None)
        }
        processed_data.append(processed_review)
    return json.dumps(processed_data)

def process_search_results(**kwargs):
    data = load_json_file('/path/to/Location_Search_California.json')
    processed_data = []
    for location in data['data']:
        processed_location = {
            'location_id': location['location_id'],
            'name': location['name'],
            'address': location['address_obj']['address_string'],
            'country': location['address_obj'].get('country'),
            'state': location['address_obj'].get('state'),
            'city': location['address_obj'].get('city')
        }
        processed_data.append(processed_location)
    return json.dumps(processed_data)

# Tasks to process JSON data
process_location_details_task = PythonOperator(
    task_id='process_location_details',
    python_callable=process_location_details,
    dag=dag,
)

process_reviews_task = PythonOperator(
    task_id='process_reviews',
    python_callable=process_reviews,
    dag=dag,
)

process_search_results_task = PythonOperator(
    task_id='process_search_results',
    python_callable=process_search_results,
    dag=dag,
)

# Tasks to create Snowflake tables
create_location_details_table = SnowflakeOperator(
    task_id='create_location_details_table',
    sql='''
    CREATE TABLE IF NOT EXISTS location_details (
        location_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(255),
        description TEXT,
        latitude FLOAT,
        longitude FLOAT,
        timezone VARCHAR(50),
        country VARCHAR(100),
        state VARCHAR(100)
    )
    ''',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

create_reviews_table = SnowflakeOperator(
    task_id='create_reviews_table',
    sql='''
    CREATE TABLE IF NOT EXISTS reviews (
        review_id VARCHAR(50) PRIMARY KEY,
        location_id VARCHAR(50),
        rating INTEGER,
        title VARCHAR(255),
        text TEXT,
        travel_date DATE,
        user_name VARCHAR(100),
        user_location VARCHAR(255)
    )
    ''',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

create_search_results_table = SnowflakeOperator(
    task_id='create_search_results_table',
    sql='''
    CREATE TABLE IF NOT EXISTS search_results (
        location_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(255),
        address VARCHAR(255),
        country VARCHAR(100),
        state VARCHAR(100),
        city VARCHAR(100)
    )
    ''',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Tasks to load data into Snowflake tables
load_location_details = SnowflakeOperator(
    task_id='load_location_details',
    sql='''
    INSERT INTO location_details (location_id, name, description, latitude, longitude, timezone, country, state)
    SELECT $1:location_id, $1:name, $1:description, $1:latitude, $1:longitude, $1:timezone, $1:country, $1:state
    FROM (SELECT PARSE_JSON(%(json_data)s))
    ''',
    params={'json_data': '{{ task_instance.xcom_pull(task_ids="process_location_details") }}'},
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

load_reviews = SnowflakeOperator(
    task_id='load_reviews',
    sql='''
    INSERT INTO reviews (review_id, location_id, rating, title, text, travel_date, user_name, user_location)
    SELECT $1:review_id, $1:location_id, $1:rating, $1:title, $1:text, 
           TO_DATE($1:travel_date, 'YYYY-MM-DD'), $1:user_name, $1:user_location
    FROM (SELECT PARSE_JSON(%(json_data)s)),
         LATERAL FLATTEN(input => $1)
    ''',
    params={'json_data': '{{ task_instance.xcom_pull(task_ids="process_reviews") }}'},
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

load_search_results = SnowflakeOperator(
    task_id='load_search_results',
    sql='''
    INSERT INTO search_results (location_id, name, address, country, state, city)
    SELECT $1:location_id, $1:name, $1:address, $1:country, $1:state, $1:city
    FROM (SELECT PARSE_JSON(%(json_data)s)),
         LATERAL FLATTEN(input => $1)
    ''',
    params={'json_data': '{{ task_instance.xcom_pull(task_ids="process_search_results") }}'},
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Define task dependencies
process_location_details_task >> create_location_details_table >> load_location_details
process_reviews_task >> create_reviews_table >> load_reviews
process_search_results_task >> create_search_results_table >> load_search_results
