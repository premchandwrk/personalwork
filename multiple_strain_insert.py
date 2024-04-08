import pandas as pd
import json
import os
from google.cloud import bigquery
from flask import Flask, jsonify, request
import pandas_gbq as pbq

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

project_id = "project-414304"
dataset_id = "BCS"

def get_table_data_as_dataframe(table_id):
    client = bigquery.Client(project=project_id)
    table = client.get_table(f"{dataset_id}.{table_id}")
    query_job = client.list_rows(table)
    df = query_job.to_dataframe()
    return df

def validate_row_data(df, row_data):
    columns = set(df.columns)
    common_cols = list(columns & set(row_data.keys()))
    values_to_check = []

    for col in common_cols:
        values_to_check.append(row_data[col])

    check = df[common_cols].isin(values_to_check).all(axis=1).any()
    return check

def filter_unwanted_rows(df, row_data):
    for col in list(row_data.keys()):
        if col not in df.columns:
            row_data.pop(col)

    return row_data

def ingest_dataframe_bq(df, table_id, schema=None):
    try:
        insert_table_id = f"{dataset_id}.{table_id}"
        df['created_date'] = pd.Timestamp.now()
        df['last_updated_timestamp'] = pd.Timestamp.now()
        pbq.to_gbq(dataframe=df, destination_table=insert_table_id, project_id=project_id, if_exists='replace', table_schema=schema)
    except Exception as e:
        raise Exception(f'Error while ingesting data: {e}')


def add_row(df, row_data,table_id, schema=None):
    row_data = filter_unwanted_rows(df, row_data)
    if not validate_row_data(df, row_data):
        # If row data is not present, add it as a new row
        df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
    else:
        # If row data is present, check if any values are different
        existing_row_index = df[list(row_data.keys())].eq(pd.Series(row_data)).all(axis=1)
        existing_row_index = existing_row_index[existing_row_index].index
        for col, val in row_data.items():
            if col not in df.columns:
                # If a new column is introduced, add it to the DataFrame
                df[col] = None
            elif col in df.columns and not (df.loc[existing_row_index, col] == val).all():
                # If an existing column's value is different, update the value
                df.loc[existing_row_index, col] = val

    # Ingest the DataFrame into BigQuery
    ingest_dataframe_bq(df, table_id, schema)
    return df

bcs_ui_all_info1_schema = [
  {"name": "bcs_code", "type": "STRING"},
  {"name": "sp_code", "type": "STRING"},
  {"name": "strain_name", "type": "STRING"},
  {"name": "scientific_name", "type": "STRING"},
  {"name": "vendor_name", "type": "STRING"},
  {"name": "genus_species", "type": "STRING"},
  {"name": "amt_of_active_ingredients", "type": "STRING"},
  {"name": "formulation_type", "type": "STRING"},
  {"name": "physical_state", "type": "STRING"},
  {"name": "formulated_or_wholebroth", "type": "STRING"},
  {"name": "product_type", "type": "STRING"},
  {"name": "synonyms", "type": "STRING"},
  {"name": "ncbi_id", "type": "STRING"},
  {"name": "chem_catlg_id", "type": "STRING"},
  {"name": "mo_catlg_id", "type": "STRING"},
  {"name": "requestor_cwid", "type": "STRING"},
  {"name": "strain_alias", "type": "STRING"}  
]

app = Flask(__name__)

@app.route('/blxde-bcs-ui/v1/insert-strain', methods=['POST'])
def insert_strain():
    try:
        data = json.loads(request.data)
        table_id = "bcs_sp_ui_all_info1"
        schema = bcs_ui_all_info1_schema

        df = get_table_data_as_dataframe(table_id)
        for json_data in data:
            for formulation_obj in json_data['formulation']:
                row = {
                    "bcs_code": json_data['bcs_code'],
                    "sp_code": json_data["sp_code"],
                    "strain_name": json_data["strain_name"],
                    "scientific_name": json_data["scientific_name"],
                    "vendor_name": json_data["vendor_name"],
                    "genus_species": json_data["genus_species"],
                    "amt_of_active_ingredients": formulation_obj['amt_of_active_ingredients'],
                    "formulation_type": formulation_obj['formulation_type'],
                    "physical_state": formulation_obj['physical_state'],
                    "formulated_or_wholebroth": formulation_obj['formulated_or_wholebroth'],
                    "product_type": formulation_obj['product_type'],
                    "synonyms": json_data["synonyms"],
                    "ncbi_id": json_data["ncbi_id"],
                    "chem_catlg_id": json_data["chem_catlg_id"],
                    "mo_catlg_id": json_data["mo_catlg_id"],
                    "requestor_cwid": json_data["requestor_cwid"],
                    "strain_alias": json_data["strain_alias"]
                }
                df = add_row(df, row,table_id, schema)

        return jsonify({'success': True, 'data': "Data Ingested Successfully!"}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 404 

if __name__ == "__main__":
    app.run()
