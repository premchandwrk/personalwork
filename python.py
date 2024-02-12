import pandas as pd
from pandas_gbq import gbq

def read_excel_to_dataframe(excel_file, sheet_name):
    """
    Read data from Excel file into a Pandas DataFrame.
    
    Args:
    - excel_file (str): Path to the Excel file.
    - sheet_name (str or int): Name or index of the sheet to read.
    
    Returns:
    - DataFrame: The DataFrame containing the data from the specified sheet.
    """
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    return df

def infer_dynamic_schema(df):
    """
    Infer the schema dynamically from a DataFrame.
    
    Args:
    - df (DataFrame): The DataFrame from which to infer the schema.
    
    Returns:
    - list of dict: The inferred schema as a list of dictionaries.
    """
    schema = []
    for col, dtype in df.dtypes.items():
        if dtype == 'int64':
            bq_type = 'INTEGER'
        elif dtype == 'float64':
            bq_type = 'FLOAT'
        else:
            bq_type = 'STRING'
        schema.append({'name': col, 'type': bq_type})
    return schema

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
    # Create table if not exists
    gbq.to_gbq(df, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='fail', table_schema=schema)

    # Insert data into BigQuery
    df.to_gbq(destination_table=f"{dataset_id}.{table_id}",
              project_id=project_id,
              if_exists='append')  # Change to 'replace' if you want to replace the data

def main():
    excel_file = 'path_to_your_excel_file.xlsx'
    sheet_name = 'your_sheet_name'
    project_id = 'your_project_id'
    dataset_id = 'your_dataset_id'
    table_id = 'your_table_id'

    df = read_excel_to_dataframe(excel_file, sheet_name)
    schema = infer_dynamic_schema(df)
    create_or_append_to_bigquery_table(df, project_id, dataset_id, table_id, schema)

if __name__ == "__main__":
    main()
