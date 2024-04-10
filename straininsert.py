
import os

key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

# Initialize BigQuery client
client = bigquery.Client()

def check_and_insert_rows(data_list, inserted_data, not_inserted_data):
    for data in data_list:
        # Define the query to check if the row exists
        query = f"""
        SELECT COUNT(*) as count
        FROM `project-414304.BCS.bcs_sp_ui_all_info`
        WHERE 
            bcs_code = '{data["bcs_code"]}' AND
            sp_code = '{data["sp_code"]}' AND
            strain_name = '{data["strain_name"]}' AND
            scientific_name = '{data["scientific_name"]}' AND
            vendor_name = '{data["vendor_name"]}' AND
            genus_species = '{data["genus_species"]}' AND
            amt_of_active_ingredients = '{data["amt_of_active_ingredients"]}' AND
            formulation_type = '{data["formulation_type"]}' AND
            physical_state = '{data["physical_state"]}' AND
            formulated_or_wholebroth = '{data["formulated_or_wholebroth"]}' AND
            product_type = '{data["product_type"]}' AND
            synonyms = '{data["synonyms"]}' AND
            ncbi_id = '{data["ncbi_id"]}' AND
            chem_catlg_id = '{data["chem_catlg_id"]}' AND
            mo_catlg_id = '{data["mo_catlg_id"]}' AND
            requestor_cwid = '{data["requestor_cwid"]}' AND
            strain_alias = '{data["strain_alias"]}'
        """

        # Run the query
        query_job = client.query(query)

        # Fetch the result
        result = query_job.result()

        # Extract count from result
        row_count = list(result)[0].count

        if row_count == 0:
            # Row doesn't exist, insert it
            insert_row(data)
            insert_data.append(data)
        else:
            # Row exists
            not_inserted_data.append(data)

@app.route('/insert_data', methods=['POST'])
def insert_data():
    data_list = request.json
    inserted_data = []
    not_inserted_data = []
    check_and_insert_rows(data_list, inserted_data, not_inserted_data)
    response = {
        "sucess": True,
        "inserted_data": inserted_data,
        "not_inserted_data": not_inserted_data
    }
    return jsonify(response)

def insert_row(data):
    # Define the BigQuery table to insert into
    table_id = "project-414304.BCS.bcs_sp_ui_all_info"

    # Create a BigQuery row
    row = {
        "bcs_code": data["bcs_code"],
        "sp_code": data["sp_code"],
        "strain_name": data["strain_name"],
        "scientific_name": data["scientific_name"],
        "vendor_name": data["vendor_name"],
        "genus_species": data["genus_species"],
        "amt_of_active_ingredients": data["amt_of_active_ingredients"],
        "formulation_type": data["formulation_type"],
        "physical_state": data["physical_state"],
        "formulated_or_wholebroth": data["formulated_or_wholebroth"],
        "product_type": data["product_type"],
        "synonyms": data["synonyms"],
        "ncbi_id": data["ncbi_id"],
        "chem_catlg_id": data["chem_catlg_id"],
        "mo_catlg_id": data["mo_catlg_id"],
        "requestor_cwid": data["requestor_cwid"],
        "strain_alias": data["strain_alias"]
    }

    # Insert the row into BigQuery
    errors = client.insert_rows_json(table_id, [row])

    if errors == []:
        print("Row inserted successfully.")
    else:
        print(f"Errors encountered: {errors}")

if __name__ == '__main__':
    app.run(debug=True)
