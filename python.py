import pandas as pd
from pandas_gbq import gbq

# Define master schema
MASTER_SCHEMA = [
    {'name': 'col1', 'type': 'STRING'},
    {'name': 'col2', 'type': 'INTEGER'},
    # Add more columns as needed
]

def read_excel_to_dataframe(excel_file, sheet_name):
    """
    Read data from Excel file into a Pandas DataFrame.
    
    Args:
    - excel_file (str): Path to the Excel file.
    - sheet_name (str or int): Name or index of the sheet to read.
    
    Returns:
    - DataFrame: The DataFrame containing the data from the specified sheet.
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        return df
    except FileNotFoundError:
        print("Error: File not found. Please provide a valid file path.")
        return None
    except pd.errors.ParserError:
        print("Error: File format is not valid. Please provide a valid Excel file.")
        return None

def create_or_append_to_bigquery_table(df, project_id, dataset_id, table_id, schema):
    """
    Create or append data to a BigQuery table.
    
    Args:
    - df (DataFrame): The DataFrame containing the data to insert.
    - project_id (str): The ID of the Google Cloud project.
    - dataset_id (str): The ID of the BigQuery dataset.
    - table_id (str): The ID of the BigQuery table.
    - schema (list of dict): The schema of the table as a list of dictionaries.
    """
    try:
        # Create table if not exists
        gbq.to_gbq(df, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='fail', table_schema=schema)

        # Filter DataFrame columns based on master schema
        df_filtered = df[[col['name'] for col in schema]]

        # Insert data into BigQuery
        df_filtered.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                            project_id=project_id,
                            if_exists='append')  # Change to 'replace' if you want to replace the data
    except Exception as e:
        print(f"An error occurred while creating or appending to the BigQuery table: {str(e)}")

def main():
    excel_file = 'path_to_your_excel_file.xlsx'
    sheet_name = 'your_sheet_name'
    project_id = 'your_project_id'
    dataset_id = 'your_dataset_id'
    table_id = 'your_table_id'

    df = read_excel_to_dataframe(excel_file, sheet_name)
    if df is not None:
        create_or_append_to_bigquery_table(df, project_id, dataset_id, table_id, MASTER_SCHEMA)

if __name__ == "__main__":
    main()
