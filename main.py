from datetime import datetime
import logging
import re
import pandas as pd
from google.cloud import bigquery
import os

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )


def transform_data(df):
    """Transform DataFrame according to requirements."""
    # Rename columns
    df.rename(columns={'productid': 'product_id', 'product': 'product_name', 'pest': 'pest_name'}, inplace=True)
    
    # Initialize an empty list to store transformed data
    transformed_data = []

    # Iterate over each row
    for index, row in df.iterrows():
        # Iterate over each column
        for col in df.columns:
            # Skip the first three columns
            if col in ['product_id', 'product_name', 'pest_name']:
                continue
            
            observation_name = col
            observation_value = row.get(col)  # Get value of column if it exists, otherwise None
            # Determine observation unit based on column name
            if col == 'feeding':
                observation_unit = 'percentage'
            elif col == 'weight':
                observation_unit = 'grams'
            elif col == 'rootgms':
                observation_unit = 'gms'
            else:
                observation_unit = 'unknown'

            # Append transformed data to the list
            transformed_data.append({'product_id': row['product_id'], 
                                     'product_name': row['product_name'],
                                     'pest_name': row.get('pest_name'),  # Get pest_name if it exists, otherwise None
                                     'observation_name': observation_name,
                                     'observation_value': observation_value,
                                     'observation_unit': observation_unit})

    # Create a DataFrame from the transformed data
    transformed_df = pd.DataFrame(transformed_data)
    return transformed_df


def table_exists(client, dataset_id, table_name):
    """Check if the BigQuery table exists."""
    try:
        client.get_table(f"{client.project}.{dataset_id}.{table_name}")
        return True
    except:
        return False

def create_bigquery_table(client, dataset_id, table_name, schema):
    """Create a BigQuery table."""
    table = bigquery.Table(f"{client.project}.{dataset_id}.{table_name}", schema=schema)
    table = client.create_table(table)
    logging.info(f"BigQuery table created: {client.project}.{dataset_id}.{table_name}")

def load_dataframe_to_bigquery(client, dataframe, dataset_id, table_name, write_disposition):
    """Load DataFrame into BigQuery table."""
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(dataframe, f"{client.project}.{dataset_id}.{table_name}", job_config=job_config)
    job.result()
    logging.info(f"Data loaded into BigQuery table: {client.project}.{dataset_id}.{table_name}")

def extract_dates_from_excel(excel_path, sheet_name):
    """Extract dates from a specific sheet in Excel."""
    # Read the Excel file into a Pandas DataFrame
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    # Define your date format regex pattern to match dates in the format dd.mm.yyyy
    date_format_regex = r'\b(\d{2})\.(\d{2})\.(\d{4})\b'

    # Create an empty list to store matched dates
    matched_dates = []

    # Iterate over each cell in the DataFrame and extract dates matching the regex pattern
    for column in df.columns:
        for cell in df[column]:
            if isinstance(cell, str):  # Check if the cell contains a string
                # Find all matches of the regex pattern in the cell
                dates_in_cell = re.findall(date_format_regex, cell)
                # Convert matched dates to datetime objects
                for date_tuple in dates_in_cell:
                    date_str = '.'.join(date_tuple)
                    matched_dates.append(datetime.strptime(date_str, '%d.%m.%Y'))

    return matched_dates

# def main():
#     # Set up logging
#     setup_logging()

#     # List of Excel files
#     excel_files = ["file1.xlsx", "file2.xlsx"]

#     # Initialize BigQuery client
#     client = bigquery.Client()

#     # Define BigQuery dataset and table names
#     dataset_id = 'onepage'
#     table_name = 'test'

#     # Define BigQuery schema for the table
#     schema = [
#         bigquery.SchemaField("product_id", "INTEGER"),
#         bigquery.SchemaField("product_name", "STRING"),
#         bigquery.SchemaField("pest_name", "STRING"),
#         bigquery.SchemaField("observation_name", "STRING"),
#         bigquery.SchemaField("observation_value", "FLOAT"),
#         bigquery.SchemaField("observation_unit", "STRING"),
#         bigquery.SchemaField("date_created", "DATE"),
#         bigquery.SchemaField("date_expired", "DATE"),
#     ]

#     for excel_file in excel_files:
#         # Read the Excel file
#         df_with_feeding_weight = pd.read_excel(excel_file, sheet_name='Sheet1')
#         # dates_df = pd.read_excel(excel_file, sheet_name='data')

#         # Check if the table already exists
#         if not table_exists(client, dataset_id, table_name):
#             # Create BigQuery tables
#             create_bigquery_table(client, dataset_id, table_name, schema)
        
#         # Transform DataFrames
#         transformed_df = transform_data(df_with_feeding_weight)

#         # Extract dates from Excel
#         dates = extract_dates_from_excel(excel_file, sheet_name='data')
        
#         # Find minimum and maximum dates
#         if dates:
#             min_date = min(dates)
#             max_date = max(dates)
            
#             # Add date information to DataFrame
#             transformed_df['date_created'] = min_date
#             transformed_df['date_expired'] = max_date

#             # Load DataFrame into BigQuery table
#             load_dataframe_to_bigquery(client, transformed_df, dataset_id, table_name, "WRITE_APPEND")

#         else:
#             print("No dates found with that format.")

def main():
    # Set up logging
    setup_logging()

    # List of Excel files
    excel_files = ["file1.xlsx", "file2.xlsx","file3.xlsx"]

    # Initialize BigQuery client
    client = bigquery.Client()

    # Define BigQuery dataset and table names
    dataset_id = 'onepage'
    table_name = 'test'

    # Define BigQuery schema for the table
    schema = [
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pest_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("observation_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("observation_value", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("observation_unit", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_created", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("date_expired", "DATE", mode="REQUIRED"),
    ]

    for excel_file in excel_files:
        # Read the Excel file
        df = pd.read_excel(excel_file, sheet_name='Sheet1')

        # Check if 'productid' column exists
        if 'productid' not in df.columns:
            logging.warning(f"Ignoring file {excel_file} as it does not contain 'productid' column.")
            continue

        # Check if the table already exists
        if not table_exists(client, dataset_id, table_name):
            # Create BigQuery tables
            create_bigquery_table(client, dataset_id, table_name, schema)
        
        # Transform DataFrame
        transformed_df = transform_data(df)

        # Extract dates from Excel
        dates = extract_dates_from_excel(excel_file, sheet_name='data')
        
        # Find minimum and maximum dates
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            
            # Add date information to DataFrame
            transformed_df['date_created'] = min_date
            transformed_df['date_expired'] = max_date

            # Load DataFrame into BigQuery table
            load_dataframe_to_bigquery(client, transformed_df, dataset_id, table_name, "WRITE_APPEND")

        else:
            print("No dates found with that format.")

if __name__ == "__main__":
    main()

