
import pandas as pd
from flask import Flask, request, jsonify
import json
import os
from google.cloud import bigquery
import pandas_gbq as pbq
from flask_swagger_ui import get_swaggerui_blueprint
from datetime import datetime


app = Flask(__name__)

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Initialize BigQuery client
client = bigquery.Client()

project_id = 'project-414304'
dataset_id = 'BCS'

# Define Swagger UI blueprint
SWAGGER_URL = '/api/docs'  # URL for accessing Swagger UI (usually /api/docs)
API_URL = '/swagger.json'  # URL for your Swagger JSON file

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "BCS UI API Documentation"
    }
)

# Register the Swagger UI blueprint
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

class Connector:
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.table = self.client.get_table(f"{dataset_id}.{table_id}")
        self.df = None
        self.columns = None

    def get_table_data_as_dataframe(self, limit=None):
        if self.table is None:
            raise Exception("Table is not connected")
        
        if self.df is not None:
            if limit is not None:
                return self.df.head(limit)
            return self.df
        
        query_job = self.client.list_rows(self.table)

        self.df = query_job.to_dataframe()
        self.set_columns()

        if limit is not None:
            return self.df.head(limit)

        return self.df
    def set_columns(self):
        self.columns = set(self.df.columns)

    def validate_row_data(self, row_data):
        common_cols = list(self.columns & set(row_data.keys()))
        values_to_check = []

        for col in common_cols:
            values_to_check.append(row_data[col])
        
        check = self.df[common_cols].isin(values_to_check).all(axis=1).any()

        return check
    # def validate_row_data(self, row_data):
    #     common_cols = list(self.columns & set(row_data.keys()))
    #     values_to_check = {}

    #     for col in common_cols:
    #         values_to_check[col] = row_data[col]
        
    #     # Check if any row in the DataFrame matches exactly with the new row data
    #     check = (self.df[list(values_to_check)] == pd.Series(values_to_check)).all(axis=1).any()

        # return check

    def filter_unwanted_rows(self, row_data):
        for col in list(row_data.keys()):
            if col not in self.columns:
                row_data.pop(col)
        
        return row_data

    def ingest_dataframe_bq(self, schema=None):
        try:
            insert_table_id = f"{self.dataset_id}.{self.table_id}"
            self.df['created_date'] = pd.Timestamp.now()
            self.df['last_updated_timestamp'] = pd.Timestamp.now()
            pbq.to_gbq(dataframe=self.df, destination_table=insert_table_id, project_id=self.project_id, if_exists='replace', table_schema=schema)
        except Exception as e:
            raise Exception(f'Error while ingesting data: {e}')
        
    # def add_row(self, row_data,schema = None):
    #     row_data = self.filter_unwanted_rows(row_data)
    #     if(self.validate_row_data(row_data)):
    #         raise Exception('Row Data is already present in table.')
    #     self.df = self.df._append(row_data, ignore_index=True)
    #     self.ingest_dataframe_bq(schema)
    # def add_row(self, row_data, schema=None):
    #     row_data = self.filter_unwanted_rows(row_data)
    #     if not self.validate_row_data(row_data):
    #         # If row data is not present, add it as a new row
    #         self.df = self.df.append(row_data, ignore_index=True)
    #     else:
    #         # If row data is present, check if any values are different
    #         existing_row_index = self.df[list(row_data.keys())].eq(pd.Series(row_data)).all(axis=1)
    #         existing_row_index = existing_row_index[existing_row_index].index
    #         for col, val in row_data.items():
    #             if col not in self.columns:
    #                 # If a new column is introduced, add it to the DataFrame
    #                 self.df[col] = None
    #                 self.columns.add(col)
    #             elif col in self.columns and not (self.df.loc[existing_row_index, col] == val).all():
    #                 # If an existing column's value is different, update the value
    #                 self.df.loc[existing_row_index, col] = val

    #     # Ingest the DataFrame into BigQuery
    #     self.ingest_dataframe_bq(schema)
    def add_row(self, row_data, schema=None):
      row_data = self.filter_unwanted_rows(row_data)
      if not self.validate_row_data(row_data):
          # If row data is not present, add it as a new row
          self.df = pd.concat([self.df, pd.DataFrame([row_data])], ignore_index=True)
      else:
          # If row data is present, check if any values are different
          existing_row_index = self.df[list(row_data.keys())].eq(pd.Series(row_data)).all(axis=1)
          existing_row_index = existing_row_index[existing_row_index].index
          for col, val in row_data.items():
              if col not in self.columns:
                  # If a new column is introduced, add it to the DataFrame
                  self.df[col] = None
                  self.columns.add(col)
              elif col in self.columns and not (self.df.loc[existing_row_index, col] == val).all():
                  # If an existing column's value is different, update the value
                  self.df.loc[existing_row_index, col] = val

      # Ingest the DataFrame into BigQuery
      self.ingest_dataframe_bq(schema)



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

def convert_to_rows(data):
    rows_to_insert = []
    for item in data:
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date = datetime.now().strftime('%Y-%m-%d')
        for form in item['formulation']:
            row = {
                'bcs_code': item['bcs_code'],
                'sp_code': item['sp_code'],
                'strain_name': item['strain_name'],
                'scientific_name': item['scientific_name'],
                'vendor_name': item['vendor_name'],
                'genus_species': item['genus_species'],
                'synonyms': item['synonyms'],
                'ncbi_id': item['ncbi_id'],
                'chem_catlg_id': item['chem_catlg_id'],
                'mo_catlg_id': item['mo_catlg_id'],
                'requestor_cwid': item['requestor_cwid'],
                'strain_alias': item['strain_alias'],
                'amt_of_active_ingredients': form['amt_of_active_ingredients'],
                'formulation_type': form['formulation_type'],
                'physical_state': form['physical_state'],
                'formulated_or_wholebroth': form['formulated_or_wholebroth'],
                'product_type': form['product_type'],
                'created_date': item.get('created_date', date),
                'timestamp': item.get('timestamp', current_timestamp)
            }
            rows_to_insert.append(row)
    return rows_to_insert

def check_existing_rows(rows_to_insert):
    # dataset_id = dataset_id # Replace 'your_dataset_id' with your dataset ID
    table_id = "bcs_sp_ui_all_info1"  # Replace 'your_table_id' with your table ID

    query = f"""
        SELECT * FROM `{dataset_id}.{table_id}`
    """

    query_job = client.query(query)
    results = query_job.result()

    existing_rows = [tuple(row.values()) for row in results]
    new_rows = [tuple(row.values()) for row in rows_to_insert]

    return [row for row in new_rows if row not in existing_rows]

######################################
@app.route('/swagger.json')
def swagger_json():
    # Define your Swagger JSON here
    swagger_json = {
        "swagger": "2.0",
        "info": {
            "title": "Your API Title",
            "version": "1.0",
            "description": "Your API Description"
        },
        "basePath": "/",
        # Add your paths and definitions here
        # Example:
        "paths": {
  "/blxde-bcs-ui/v1/insert-strain": {
    "post": {
      "summary": "Insert a strain",
      "description": "Endpoint to insert strain data into the database",
      "parameters": [
        {
          "name": "body",
          "in": "body",
          "description": "Strain data to insert",
          "required": True,
          "schema": {
            "type": "object",
            "properties": {
              "bcs_code": {"type": "string"},
              "sp_code": {"type": "string"},
              "strain_name": {"type": "string"},
              "scientific_name": {"type": "string"},
              "vendor_name": {"type": "string"},
              "genus_species": {"type": "string"},
              "synonyms": {"type": "string"},
              "ncbi_id": {"type": "string"},
              "chem_catlg_id": {"type": "string"},
              "mo_catlg_id": {"type": "string"},
              "requestor_cwid": {"type": "string"},
              "strain_alias": {"type": "string"},
              "formulation": {
                "type": "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "amt_of_active_ingredients": {"type": "number"},
                    "formulation_type": {"type": "string"},
                    "physical_state": {"type": "string"},
                    "formulated_or_wholebroth": {"type": "string"},
                    "product_type": {"type": "string"}
                  },
                  "required": ["amt_of_active_ingredients", "formulation_type", "physical_state", "formulated_or_wholebroth", "product_type"]
                }
              }
            },
            "required": ["bcs_code", "sp_code", "strain_name", "scientific_name", "vendor_name", "genus_species", "synonyms", "ncbi_id", "chem_catlg_id", "mo_catlg_id", "requestor_cwid", "strain_alias", "formulation"]
          }
        }
      ],
      "responses": {
        "200": {
          "description": "Data Ingested Successfully!",
          "schema": {
            "type": "object",
            "properties": {
              "success": {"type": "boolean", "default": True},
              "data": {"type": "string"}
            },
            "required": ["success", "data"]
          }
        },
        "404": {
          "description": "Error occurred",
          "schema": {
            "type": "object",
            "properties": {
              "success": {"type": "boolean", "default": False},
              "error": {"type": "string"}
            },
            "required": ["success", "error"]
          }
        }
      }
    }
  }
}

    }
    return jsonify(swagger_json)

################################
@app.route('/blxde-bcs-ui/v1/insert-strain', methods=['POST'])
def insert_strain():
    try:
        # token = request.headers.get('token', None)
        # # Assuming you have a function decode_token to decode the token
        # decoded_token = decode_token(token)
        # exp_time = decoded_token.get('exp')
        # if token_expire(exp_time):
        #     raise Exception('Token is expired.')
        
        data = json.loads(request.data)
        table_id = "bcs_sp_ui_all_info1"
        schema = bcs_ui_all_info1_schema  # Assuming you have defined this schema
        
        rows_to_insert = []
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
                rows_to_insert.append(row)

        bq_connector = Connector(project_id, dataset_id, table_id)
        df = bq_connector.get_table_data_as_dataframe()
        for row in rows_to_insert:
            bq_connector.add_row(row, schema)

        # bq_connector.ingest_dataframe_bq(schema)

        return jsonify({'success': True, 'data': "Data Ingested Successfully!"}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 404 
     

if __name__ == "__main__":
    app.run(debug=True)
