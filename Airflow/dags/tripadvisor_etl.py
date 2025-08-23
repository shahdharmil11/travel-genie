from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from apify_client import ApifyClient
import json
from typing import Dict, List

# Constants
ACTOR_ID = "dbEyMBriog95Fv8CW"
SNOWFLAKE_CONN_ID = "snowflake_default"

# SQL statements for creating tables
create_hotels_table = """
CREATE TABLE IF NOT EXISTS hotels (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    rating FLOAT,
    num_reviews INTEGER,
    location_string VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    price_level VARCHAR(50),
    ranking_position INTEGER,
    phone VARCHAR(50),
    address VARCHAR(500),
    website VARCHAR(500),
    email VARCHAR(255),
    hotel_class FLOAT,
    number_of_rooms INTEGER,
    amenities TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
"""

create_attractions_table = """
CREATE TABLE IF NOT EXISTS attractions (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    rating FLOAT,
    num_reviews INTEGER,
    location_string VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    price_level VARCHAR(50),
    ranking_position INTEGER,
    phone VARCHAR(50),
    address VARCHAR(500),
    website VARCHAR(500),
    email VARCHAR(255),
    subcategories TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
"""

create_restaurants_table = """
CREATE TABLE IF NOT EXISTS restaurants (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    rating FLOAT,
    num_reviews INTEGER,
    location_string VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    price_level VARCHAR(50),
    ranking_position INTEGER,
    phone VARCHAR(50),
    address VARCHAR(500),
    website VARCHAR(500),
    email VARCHAR(255),
    cuisines TEXT,
    dietary_restrictions TEXT,
    meals TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
"""

# SQL to create stages
create_stages = """
CREATE OR REPLACE STAGE hotels_stage;
CREATE OR REPLACE STAGE attractions_stage;
CREATE OR REPLACE STAGE restaurants_stage;
"""

def get_apify_client():
    """Get Apify client with credentials from Airflow connection"""
    apify_conn = BaseHook.get_connection('apify')
    return ApifyClient(apify_conn.password)

def fetch_tripadvisor_data(**context) -> Dict:
    """Fetch data using Apify TripAdvisor actor"""
    client = get_apify_client()
    
    run_input = {
        "query": "Chicago, Illnois, USA",
        "maxItemsPerQuery": 1000,
        "includeTags": True,
        "includeNearbyResults": False,
        "includeAttractions": True,
        "includeRestaurants": True,
        "includeHotels": True,
        "includeVacationRentals": False,
        "checkInDate": "",
        "checkOutDate": "",
        "includePriceOffers": False,
        "includeAiReviewsSummary": False,
        "language": "en",
        "currency": "USD",
        "locationFullName": "Chicago, Illnois, USA",
    }
    
    run = client.actor(ACTOR_ID).call(run_input=run_input)
    
    items = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        items.append(item)
    
    context['task_instance'].xcom_push(key='raw_data', value=items)
    return items

def process_and_separate_data(**context) -> Dict[str, List]:
    """Process raw data and separate into different categories"""
    raw_data = context['task_instance'].xcom_pull(key='raw_data')
    
    hotels = []
    attractions = []
    restaurants = []
    
    for item in raw_data:
        base_fields = {
            'id': item.get('id'),
            'name': item.get('name'),
            'rating': item.get('rating'),
            'num_reviews': item.get('numberOfReviews'),
            'location_string': item.get('locationString'),
            'latitude': item.get('latitude'),
            'longitude': item.get('longitude'),
            'price_level': item.get('priceLevel'),
            'ranking_position': item.get('rankingPosition'),
            'phone': item.get('phone'),
            'address': item.get('address'),
            'website': item.get('website'),
            'email': item.get('email')
        }
        
        if item.get('type') == 'HOTEL':
            hotels.append({
                **base_fields,
                'hotel_class': item.get('hotelClass'),
                'number_of_rooms': item.get('numberOfRooms'),
                'amenities': ','.join(item.get('amenities', []))
            })
            
        elif item.get('type') == 'ATTRACTION':
            attractions.append({
                **base_fields,
                'subcategories': ','.join(item.get('subcategories', [])),
                'description': item.get('description')
            })
            
        elif item.get('type') == 'RESTAURANT':
            restaurants.append({
                **base_fields,
                'cuisines': ','.join(item.get('cuisines', [])),
                'dietary_restrictions': ','.join(item.get('dietaryRestrictions', [])),
                'meals': ','.join(item.get('mealTypes', []))
            })
    
    result = {
        'hotels': hotels,
        'attractions': attractions,
        'restaurants': restaurants
    }
    
    context['task_instance'].xcom_push(key='processed_data', value=result)
    return result

def generate_copy_into_sql(category: str, data: List[Dict]) -> str:
    """Generate the appropriate COPY INTO statement based on category"""
    if category == 'hotels':
        return """
        COPY INTO hotels (id, name, rating, num_reviews, location_string, 
                         latitude, longitude, price_level, ranking_position,
                         phone, address, website, email, hotel_class,
                         number_of_rooms, amenities)
        FROM (SELECT 
            $1:id::STRING,
            $1:name::STRING,
            $1:rating::FLOAT,
            $1:num_reviews::INTEGER,
            $1:location_string::STRING,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT,
            $1:price_level::STRING,
            $1:ranking_position::INTEGER,
            $1:phone::STRING,
            $1:address::STRING,
            $1:website::STRING,
            $1:email::STRING,
            $1:hotel_class::FLOAT,
            $1:number_of_rooms::INTEGER,
            $1:amenities::STRING
        FROM @HOTELS_STAGE)
        FILE_FORMAT = (TYPE = 'JSON');
        """
    elif category == 'attractions':
        return """
        COPY INTO attractions (id, name, rating, num_reviews, location_string,
                             latitude, longitude, price_level, ranking_position,
                             phone, address, website, email, subcategories,
                             description)
        FROM (SELECT 
            $1:id::STRING,
            $1:name::STRING,
            $1:rating::FLOAT,
            $1:num_reviews::INTEGER,
            $1:location_string::STRING,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT,
            $1:price_level::STRING,
            $1:ranking_position::INTEGER,
            $1:phone::STRING,
            $1:address::STRING,
            $1:website::STRING,
            $1:email::STRING,
            $1:subcategories::STRING,
            $1:description::STRING
        FROM @ATTRACTIONS_STAGE)
        FILE_FORMAT = (TYPE = 'JSON');
        """
    else:  # restaurants
        return """
        COPY INTO restaurants (id, name, rating, num_reviews, location_string,
                             latitude, longitude, price_level, ranking_position,
                             phone, address, website, email, cuisines,
                             dietary_restrictions, meals)
        FROM (SELECT 
            $1:id::STRING,
            $1:name::STRING,
            $1:rating::FLOAT,
            $1:num_reviews::INTEGER,
            $1:location_string::STRING,
            $1:latitude::FLOAT,
            $1:longitude::FLOAT,
            $1:price_level::STRING,
            $1:ranking_position::INTEGER,
            $1:phone::STRING,
            $1:address::STRING,
            $1:website::STRING,
            $1:email::STRING,
            $1:cuisines::STRING,
            $1:dietary_restrictions::STRING,
            $1:meals::STRING
        FROM @RESTAURANTS_STAGE)
        FILE_FORMAT = (TYPE = 'JSON');
        """

def load_to_snowflake(**context):
    """Load processed data into Snowflake tables"""
    import tempfile
    import os
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    processed_data = context['task_instance'].xcom_pull(key='processed_data')
    
    # Create hook with warehouse parameter
    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse='ANIMAL_TASK_WH'  # Use your actual warehouse name
    )
    
    for category, data in processed_data.items():
        if data:  # Only process if we have data for this category
            # Create a temporary file to store the JSON data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                # Write each record as a separate line in the JSON file
                for record in data:
                    json.dump(record, f)
                    f.write('\n')
                temp_file_path = f.name
            
            try:
                # PUT file into stage
                stage_name = f"{category.upper()}_STAGE"
                put_command = f"PUT file://{temp_file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                hook.run(put_command)
                
                # Generate and execute COPY INTO command
                copy_sql = generate_copy_into_sql(category, data)
                hook.run(copy_sql)
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 5)
}

dag = DAG(
    'tripadvisor_actor_etl',
    default_args=default_args,
    description='ETL pipeline for TripAdvisor data using Apify Actor',
    schedule_interval='@daily',
    catchup=False
)

# Task definitions
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_tripadvisor_data,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_and_separate_data,
    dag=dag
)

create_stages = SnowflakeOperator(
    task_id='create_stages',
    sql=create_stages,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

create_tables = SnowflakeOperator(
    task_id='create_tables',
    sql=[create_hotels_table, create_attractions_table, create_restaurants_table],
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_snowflake,
    dag=dag
)

# Set task dependencies
fetch_data >> process_data >> create_stages >> create_tables >> load_data