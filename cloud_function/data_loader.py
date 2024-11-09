import os
import pandas as pd
from google.cloud.sql.connector import Connector
import sqlalchemy
from google.cloud import storage
from io import StringIO
import numpy as np

# Cloud SQL configuration
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

storage_client = storage.Client()


def download_csv_from_gcs(bucket_name, file_name):
    # Downloads CSV from Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_data = blob.download_as_string().decode("utf-8")
    return StringIO(csv_data)


def clean_data(df):
    # Deduplicate 'product_upc' column if it exists
    if 'product_upc' in df:
        df = df.drop_duplicates(subset='product_upc', keep="last").reset_index(drop=True)
        df['product_upc'] = df['product_upc'].astype("string")

    if 'item_number' in df:
        df['item_number'] = df['item_number'].astype("string")
    # Unification of Null values data type
    df = df.replace(r'', np.nan, regex=True).replace({'None': np.nan, None: np.nan})
    return df


def process_meta_data(csv_file_like):
    # Processes 'meta' CSV file with custom parsing and cleaning
    data = []
    for line in csv_file_like:
        columns = line.strip().split(",", 6)  # Split on first 6 commas
        data.append(columns)

    header, rows = data[0], data[1:]
    df = pd.DataFrame(rows, columns=header)
    # Unification of Null values data type
    df = clean_data(df)

    # Fix misplaced department data
    mask = df['department'].apply(lambda x: isinstance(x, str)) & df['supplier'].isna()
    df.loc[mask, 'supplier'] = df.loc[mask, 'department']
    df.loc[mask, 'department'] = np.nan

    # Convert data types
    df['department'] = df['department'].astype('Int64')
    df['case_pack'] = df['case_pack'].astype(float)

    return df


def load_sql_file(file_path, **kwargs):
    # Loads an SQL file, injects variables, and returns the formatted query string
    with open(file_path, 'r') as file:
        query = file.read()
    # Substitute placeholders with actual values from kwargs
    return query.format(**kwargs)


def main(data, context):
    # Get bucket and file name from event data
    bucket_name = data["bucket"]
    file_name = data["name"]
    print(f'Processing: {file_name}')

    table_name = file_name.replace('coding_challenge_', '').replace('.csv', '')
    csv_file_like = download_csv_from_gcs(bucket_name, file_name)

    # Load CSV into DataFrame and clean data
    if table_name == 'meta':
        df = process_meta_data(csv_file_like)
    else:
        df = pd.read_csv(csv_file_like)
        df = clean_data(df)

    # Setup SQL connection
    connector = Connector()
    with connector.connect(
            INSTANCE_CONNECTION_NAME, "pymysql", user=DB_USER, password=DB_PASS, db=DB_NAME
    ) as conn:
        engine = sqlalchemy.create_engine("mysql+pymysql://", creator=lambda: conn)

        # Write DataFrame to SQL table
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Data loaded successfully from {file_name} to Cloud SQL.")

        # Run SQL to create/update product and product_alternates tables
        temp_table_query = load_sql_file("create_temp_table.sql")
        update_products_query = load_sql_file("update_products_table.sql")
        create_alternates_query = load_sql_file("create_product_alternates_table.sql")

        # Execute SQL commands
        sql_commands = [temp_table_query, update_products_query, create_alternates_query]

        for query in (''.join(sql_commands)).strip().rstrip(';').split(';'):
            with conn.cursor() as cursor:
                cursor.execute(query)
