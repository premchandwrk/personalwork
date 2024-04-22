
from flask import Flask, request, jsonify
from google.cloud import bigquery
import concurrent.futures
import os
from datetime import datetime
import time

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

app = Flask(__name__)

# Initialize BigQuery client
client = bigquery.Client()

executor = concurrent.futures.ThreadPoolExecutor()

def row_exists(data_list):
    table_id = "project-414304.BCS.bcs_sp_ui_all_info3"  # Adjust the table ID as per your setup

    # Convert data list to a list of strings representing tuples
    identifiers = [str(tuple(data.values())) for data in data_list]

    # Construct the IN clause with formatted tuples
    in_clause = ",".join(identifiers)

    # Construct the SQL query
    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE (bcs_code, sp_code, strain_name, scientific_name, vendor_name,
               genus_species, amt_of_active_ingredients, formulation_type,
               physical_state, formulated_or_wholebroth, product_type,
               synonyms, ncbi_id, chem_catlg_id, mo_catlg_id, requestor_cwid,
               strain_alias) IN ({in_clause})
            """

    # Execute the query
    query_job = client.query(query)
    result = query_job.result()

    # Check if any matching rows exist
    row_count = sum(1 for _ in result)

    return row_count > 0


def get_max_id():
    # Define the query to get the maximum ID
    query = """
        SELECT MAX(id) as max_id FROM `project-414304.BCS.bcs_sp_ui_all_info3`
    """
    # Run the query
    query_job = client.query(query)

    # Fetch the maximum ID
    result = list(query_job.result())
    max_id = result[0]['max_id'] if result else 0

    return max_id

def insert_rows_batch(data_list):
    # Get the maximum existing ID
    max_id = get_max_id()

    # Define the BigQuery table to insert into
    table_id = "project-414304.BCS.bcs_sp_ui_all_info3"
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Create rows for batch insertion with incremental IDs
    rows_to_insert = []
    for i, data in enumerate(data_list):
        row = {
            "id": max_id + i + 1,  # Increment the ID from the maximum ID
            **data,
            "created_date": current_date,
            "last_updated_timestamp": current_time
        }
        rows_to_insert.append(row)

    # Insert rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        print("Batch of rows inserted successfully.")
    else:
        print(f"Errors encountered: {errors}")

def insert_data_async(data_list):
    future = executor.submit(insert_rows_batch, data_list)
    return future

# Define a function to get the current time in milliseconds
current_milli_time = lambda: int(round(time.time() * 1000))

@app.route('/insert_data', methods=['POST'])
def insert_data():
    start_time = current_milli_time()  # Record start time
    data_list = request.json
    existing_rows = row_exists(data_list)
    
    inserted_data = []
    not_inserted_data = []

    # Filter data based on existing rows
    for data in data_list:
        if existing_rows:
            not_inserted_data.append(data)
        else:
            inserted_data.append(data)

    # If there is data for insertion, perform insertion asynchronously
    if inserted_data:
        insert_data_async(inserted_data)

    end_time = current_milli_time()  # Record end time
    time_taken_secs = (end_time - start_time) / 1000  # Calculate time taken in seconds

    response = {
        "success": True,
        "inserted_data": inserted_data,
        "not_inserted_data": not_inserted_data,
        "time_taken_secs": time_taken_secs  # Include time taken in the response
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
