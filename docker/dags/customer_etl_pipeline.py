from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import os


# File path to the local CSV file
MYSQL_CONN_ID = 'mysql_conn'  # Make sure this matches the Airflow connection name
MYSQL_TABLE_NAME = 'orders'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(dag_id='customer_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_data():
        """Extract data from MySQL database running in Docker using Airflow's MySQL connection."""
        # mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        # connection = mysql_hook.get_connection(MYSQL_CONN_ID)

        # Retrieve connection parameters dynamically from Airflow
        MYSQL_HOST = 'host.docker.internal'
        MYSQL_PORT =  3306 
        MYSQL_USER = 'Suraj'
        MYSQL_PASSWORD = 'root'
        MYSQL_DB_NAME = 'sample_orders'

        # Create SQLAlchemy engine using the connection details
        connection_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
        engine = create_engine(connection_uri)

        # Read data from MySQL into Pandas DataFrame
        df_db = pd.read_sql(f"SELECT * FROM {MYSQL_TABLE_NAME}", con=engine)

        # Convert DataFrame to JSON and return
        return df_db.to_json()
    
    @task()
    def transform_data(data):
        """Transform data: Cleaning and filtering."""
        df = pd.read_json(data)
        
        # Example transformation: remove rows with missing values
        df.dropna(inplace=True)
        
        # Example transformation: convert column names to lowercase
        df.columns = df.columns.str.lower()

        # Convert order_date to proper DATE format (if not already in the correct format)
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        
        return df.to_json()
    
    @task()
    def create_table():
        """Create table in PostgreSQL if it doesn't exist."""
        connection = PostgresHook(postgres_conn_id='postgres_default').get_conn()
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cleaned_sample_orders (
            order_id INT PRIMARY KEY,
            customer_name VARCHAR(50),
            product_name VARCHAR(50),
            quantity INT,
            price INT,
            order_date DATE
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
        print("Table checked/created successfully in PostgreSQL.")
    
    @task()
    def load_data(transformed_data):
        """Load transformed data into PostgreSQL database."""
        df = pd.read_json(transformed_data)

        # Check if 'order_date' is in milliseconds (large values)
        if df['order_date'].dtype == 'int64' and df['order_date'].max() > 1e10:  # Check if it's likely in milliseconds

           df['order_date'] = df['order_date'] / 1000  # Convert milliseconds to seconds
        df['order_date'] = pd.to_datetime(df['order_date'], unit='s').dt.date  # Convert to date format
        

        # Establish PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # Insert data row by row
        for _, row in df.iterrows():

           # Check if 'order_date' is in milliseconds (large values)
    
            cursor.execute("""
            INSERT INTO cleaned_sample_orders (order_id, customer_name, product_name, quantity, price, order_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;  -- Prevents duplicate inserts
            """, (
                row['order_id'],
                row['customer_name'],
                row['product_name'],
                row['quantity'],
                row['price'],
                row['order_date']
            ))

        # Commit and close connection
        connection.commit()
        cursor.close()
        connection.close()
        
        print("Data loaded into PostgreSQL successfully.")
            
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    table_created = create_table()  # Ensure table exists
    load_data_task = load_data(transformed_data)

    # Set the correct order of execution
    transformed_data >> table_created >> load_data_task
