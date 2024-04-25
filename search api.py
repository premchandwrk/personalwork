from app import app
import os
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

client = bigquery.Client()
project_id = "project-414304"

@app.route('/search', methods=['POST'])
def search():
    dataset_id = "BCS"
    table_id = "bcs_sp_ui_all_info3"
    data = request.json
    keywords = data.get('keyword')
    if not isinstance(keywords, list):
        return jsonify({"error": "Keywords must be provided as a list"}), 400
    
    schema = get_table_schema(dataset_id, table_id)
    keyword_conditions = []

    for keyword in keywords:
        keyword_condition = ' OR '.join([f'CAST({col} AS STRING) LIKE "%{keyword}%"' for col in schema])
        keyword_conditions.append(f'({keyword_condition})')

    query = f"SELECT * FROM `{dataset_id}.{table_id}` WHERE {' OR '.join(keyword_conditions)}"
    results = client.query(query).result()
    return jsonify([dict(row) for row in results])

def get_table_schema(dataset_id, table_id):
    table = client.get_table(f"{dataset_id}.{table_id}")
    return [field.name for field in table.schema]
