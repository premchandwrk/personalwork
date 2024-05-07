from flask import jsonify, request
from app import app
import time
from google.cloud import bigquery
import os
from datetime import datetime

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Initialize BigQuery client
client = bigquery.Client()

def row_exists(data_list):
    table_id = "project-414304.BCS.bcs_sp_ui_all_info3"
    identifiers = [str(tuple(data.values())) for data in data_list]
    in_clause = ",".join(identifiers)
    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE (bcs_code, sp_code, strain_name, scientific_name, vendor_name,
               genus_species, amt_of_active_ingredients, formulation_type,
               physical_state, formulated_or_wholebroth, product_type,
               synonyms, ncbi_id, chem_catlg_id, mo_catlg_id, requestor_cwid,
               strain_alias) IN ({in_clause})
            """
    query_job = client.query(query)
    result = query_job.result()
    return any(result)

def get_max_id():
    query = """
        SELECT MAX(id) as max_id FROM `project-414304.BCS.bcs_sp_ui_all_info3`
    """
    query_job = client.query(query)
    result = list(query_job.result())
    max_id = result[0]['max_id'] if result else 0
    return max_id

# from google.api_core.exceptions import BadRequest

# def process_data(data_list):
#     max_id = get_max_id()
#     table_id = "project-414304.BCS.bcs_sp_ui_all_info3"
#     current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     current_date = datetime.now().strftime("%Y-%m-%d")
    
#     inserted_data = []
#     updated_data = []
#     not_inserted_data = []
    
#     existing_data = set()
#     for data in data_list:
#         if 'id' in data:
#             updated_data.append(data)
#         elif row_exists([data]):
#             not_inserted_data.append(data)        
#         else:
#             max_id += 1
#             data['id'] = max_id
#             inserted_data.append(data)
#             existing_data.add(tuple(data.values()))
    
#     rows_to_insert_with_timestamps = [{
#         **data,
#         "created_date": current_date,
#         "last_updated_timestamp": current_time
#     } for data in inserted_data]
    
#     # Handle the case where there are no rows to insert
#     if rows_to_insert_with_timestamps:
#         errors_insert = client.insert_rows_json(table_id, rows_to_insert_with_timestamps)
#     else:
#         errors_insert = []
    
#     errors_update = []
    
#     for data in updated_data:
#         primary_key = data.pop('id')
#         data["last_updated_timestamp"] = current_time
#         print(data)
#         # Construct the update query
#         # Convert values to appropriate data types
#         for key, value in data.items():
#             if isinstance(value, str):
#                 data[key] = f"'{value}'"  # Wrap string values in single quotes
        
#         # Construct the update query
#         update_query = f"""
#             UPDATE `{table_id}`
#             SET {', '.join([f"`{key}` = {value}" for key, value in data.items()])}
#             WHERE id = {primary_key}
#         """
#         try:
#             client.query(update_query).result()
#         except Exception as e:
#             errors_update.append(str(e))
    
#     if not errors_insert and not errors_update:
#         print("All rows inserted/updated successfully.")
#     else:
#         print(f"Errors encountered during insertion/update.{errors_update}")
    
#     return inserted_data, updated_data, not_inserted_data

def process_data(data_list):
    max_id = get_max_id()
    table_id = "project-414304.BCS.bcs_sp_ui_all_info3"
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    inserted_data = []
    updated_data = []
    not_inserted_data = []
    
    existing_data = set()
    for data in data_list:
        if 'id' in data:
            updated_data.append(data)
        elif row_exists([data]):
            not_inserted_data.append(data)
        else:
            max_id += 1
            data['id'] = max_id
            inserted_data.append(data)
            existing_data.add(tuple(data.values()))
    
    rows_to_insert_with_timestamps = [{
        **data,
        "created_date": current_date,
        "last_updated_timestamp": current_time
    } for data in inserted_data]
    
    # Handle the case where there are no rows to insert
    if rows_to_insert_with_timestamps:
        errors_insert = client.insert_rows_json(table_id, rows_to_insert_with_timestamps)
    else:
        errors_insert = []
    
    # Introduce a delay to allow the streaming buffer to flush
    time.sleep(2)
    
    errors_update = []
    
    for data in updated_data:
        primary_key = data.pop('id')
        data["last_updated_timestamp"] = current_time
        
        # Convert values to appropriate data types
        for key, value in data.items():
            if isinstance(value, str):
                data[key] = f"'{value}'"  # Wrap string values in single quotes
        
        # Construct the update query
        update_query = f"""
            UPDATE `{table_id}`
            SET {', '.join([f"`{key}` = {value}" for key, value in data.items()])}
            WHERE id = {primary_key} and  last_updated_timestamp < TIMESTAMP_SUB(TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'Asia/Kolkata')), INTERVAL 13 MINUTE);
        """
        try:
            client.query(update_query).result()
        except Exception as e:
            errors_update.append(str(e))
    
    if not errors_insert and not errors_update:
        print("All rows inserted/updated successfully.")
    else:
        print("Errors encountered during insertion/update.")
    
    return inserted_data, updated_data, not_inserted_data

@app.route('/insert_data', methods=['POST'])
def insert_data():
    start_time = time.time()
    data_list = request.json
    inserted_data, updated_data, not_inserted_data = process_data(data_list)
    end_time = time.time()
    time_taken_secs = end_time - start_time
    response = {
        "success": True,
        "inserted_data": inserted_data,
        "updated_data": updated_data,
        "not_inserted_data": not_inserted_data,
        "time_taken_secs": time_taken_secs
    }
    return jsonify(response)
