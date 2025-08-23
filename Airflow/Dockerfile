FROM apache/airflow:2.7.1-python3.11

USER airflow
RUN pip install --upgrade pip
RUN pip install apache-airflow snowflake-connector-python snowflake-sqlalchemy apache-airflow-providers-snowflake apify-client
