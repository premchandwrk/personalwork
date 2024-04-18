from flask import Flask, request, jsonify
from google.cloud import bigquery
import os
from datetime import datetime

# Initialize Flask app
app = Flask(__name__)

# Initialize BigQuery client
client = bigquery.Client()

# Set up environment variable for Google Cloud key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

# Define BigQuery table ID
table_id = "project-414304.BCS.bcs_sp_ui_all_info2"

# Function to insert rows in batch
def insert_rows(rows_to_insert):
    # Insert rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        print("Batch of rows inserted successfully.")
    else:
        print(f"Errors encountered: {errors}")

# Function to check if rows exist in BigQuery
def rows_exist(data_list):
    identifiers = [tuple(data.values()) for data in data_list]
    query = f"""
        SELECT COUNT(*)
        FROM `{table_id}`
        WHERE (bcs_code, sp_code, strain_name, scientific_name, vendor_name,
               genus_species, amt_of_active_ingredients, formulation_type,
               physical_state, formulated_or_wholebroth, product_type,
               synonyms, ncbi_id, chem_catlg_id, mo_catlg_id, requestor_cwid,
               strain_alias) IN ({",".join(["('%s')" % "','".join(map(str, row)) for row in identifiers])})
    """
    query_job = client.query(query)
    result = query_job.result()
    row_count = next(result)[0]
    return row_count > 0

# Route for inserting data
@app.route('/insert_data', methods=['POST'])
def insert_data():
    data_list = request.json
    inserted_data = []
    not_inserted_data = []

    # Check if rows already exist
    if rows_exist(data_list):
        not_inserted_data = data_list
    else:
        # Fetch maximum existing ID
        max_id_query = client.query(
            f"SELECT MAX(id) as max_id FROM `{table_id}`"
        )
        max_id = max_id_query.to_dataframe()["max_id"][0] or 0

        # Current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Construct rows for batch insertion
        rows_to_insert = []
        for i, data in enumerate(data_list):
            row = {
                "id": int(max_id) + i + 1,
                "created_date": current_time,
                "last_updated_timestamp": current_time
            }
            row.update(data)
            rows_to_insert.append(row)

        # Insert rows in batch
        if rows_to_insert:
            insert_rows(rows_to_insert)
            inserted_data = rows_to_insert

    response = {
        "success": True,
        "inserted_data": inserted_data,
        "not_inserted_data": not_inserted_data
    }

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
