from flask import Flask, request, jsonify
from flasgger import Swagger
import jwt
import json
from google.cloud import bigquery

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

app = Flask(__name__)
swagger = Swagger(app)
client = bigquery.Client()

def load_table(table_name):
    client = bigquery.Client()
    table_ref = client.dataset('onepage').table(table_name)
    table = client.get_table(table_ref)
    rows = client.list_rows(table)
    return rows

# Load the user_access data from BigQuery
def load_user_access():
    client = bigquery.Client()
    query = """
        SELECT * FROM `project-414304.onepage.user_access`
    """
    query_job = client.query(query)
    results = query_job.result()
    user_access_data = {}
    for row in results:
        user_access_data[row.CWID] = {'user_name': row.user_name, 'email': row.email, 'access_group': row.access_group}
    print(user_access_data)
    return user_access_data

# API endpoint for user login
@app.route('/login', methods=['POST'])
def login():
    """
    User login endpoint.
    ---
    parameters:
      - name: token
        in: body
        type: string
        required: true
        description: JWT token for user authentication
    responses:
      200:
        description: Login successful
      500:
        description: Internal server error
    """
    try:
        # Get JWT token from request
        token = request.json['token']
        
        # Decode JWT token to get CWID
        decoded_token = jwt.decode(token, algorithms = ['HS384'], verify = False)
        cwid = decoded_token.get('cwid')
        
        # Load user access data
        user_access_data = load_user_access()
        
        # Check if CWID exists in user access data
        if cwid in user_access_data:
            user_info = user_access_data[cwid]
            response = {
                'status': 'success',
                'message': 'Login successful',
                'user_name': user_info['user_name'],
                'email': user_info['email'],
                'access_group': user_info['access_group']
            }
        else:
            response = {
                'status': 'error',
                'message': 'User is not valid'
            }
        
        return jsonify(response), 200
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500


#############################################
@app.route('/get_product_data', methods = ['GET'])
def get_product_data():
    """
    Get product data endpoint.
    ---
    responses:
      200:
        description: Product data retrieved successfully
      500:
        description: Internal server error
    """
    try:
        # Get JWT token from request
        # token = request.json['token']

        product_table = "Product"

        data = load_table(product_table)

         # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in data]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500    

#############################################
@app.route('/get_crop_data', methods = ['GET'])
def get_crop_data():
    """
    Get crop data endpoint.
    ---
    responses:
      200:
        description: crop data retrieved successfully
      500:
        description: Internal server error
    """
    try:
        # Get JWT token from request
        # token = request.json['token']

        product_table = "Crop"

        data = load_table(product_table)
         # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in data]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500 

#############################################
@app.route('/get_disease_data', methods = ['GET'])
def get_disease_data():
    """
    Get disease data endpoint.
    ---
    responses:
      200:
        description: disease data retrieved successfully
      500:
        description: Internal server error
    """
    try:
        # Get JWT token from request
        # token = request.json['token']

        product_table = "Disease"

        data = load_table(product_table)

         # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in data]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500  

#############################################
@app.route('/get_pest_data', methods = ['GET'])
def get_pest_data():
    """
    Get pest data endpoint.
    ---
    responses:
      200:
        description: pest data retrieved successfully
      500:
        description: Internal server error
    """
    try:
        # Get JWT token from request
        # token = request.json['token']

        product_table = "Pest"

        data = load_table(product_table)

         # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in data]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500

################################################################
@app.route('/get_product_details', methods = ['GET'])
def get_product_crop_details():
    """
    Get get_product_crop_details data endpoint.
    ---
    responses:
      200:
        description: get_product_crop_details retrieved successfully
      500:
        description: Internal server error
    """
    try:
        query = """SELECT p.Product_ID ,p.Product_Name, ARRAY_AGG(c.Crop_Name) as crops ,ARRAY_AGG(d.Disease_Name) as disesases, ARRAY_AGG(pe.Pest_Name) as pests
                    FROM `project-414304.onepage.Product` as p 
                    inner join `project-414304.onepage.productcrop` as pc 
                    on p.Product_ID = pc.Product_ID 
                    inner join `project-414304.onepage.Crop` as c
                    on c.Crop_ID = pc.Crop_ID
                    inner join `project-414304.onepage.productdisease` as pd
                    on pd.Product_ID = p.Product_ID
                    inner join `project-414304.onepage.Disease` as d
                    on pd.Disease_ID = d.Disease_ID
                    inner join `project-414304.onepage.productpest` as ppe
                    on ppe.Product_ID = p.Product_ID
                    inner join `project-414304.onepage.Pest` as pe
                    on ppe.Pest_ID = pe.Pest_ID
                    group by p.Product_Name, p.Product_ID"""
        
        query_job = client.query(query)
        results = query_job.result()

        # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in results]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500
    
@app.route('/get_all_document_details', methods = ['GET'])
def get_all_document_details():
    """
    Get get_all_document_details data endpoint.
    ---
    responses:
      200:
        description: get_all_document_details retrieved successfully
      500:
        description: Internal server error
    """
    try:
        query = """SELECT  
                Document.Title,
                Document.Filetype,
                Document.Modified,
                Document.Year,
                Document.Country,
                Document.Classification,
                Document.Category_for_one_page,
                Document.Application_type,
                Document.Path,
                Document.Filesize,
                Document.Language,
                Product.Product_Name,
                ARRAY_AGG(Disease.Disease_Name) AS Disease_Names,
                ARRAY_AGG(Crop.Crop_Name) AS Crop_Names,
                ARRAY_AGG(Pest.Pest_Name) AS Pest_Names
            FROM
                `project-414304.onepage.Document` AS Document
            LEFT JOIN
                UNNEST(SPLIT(Document.Product_ID, ',')) AS Product_ID
                LEFT JOIN `project-414304.onepage.Product` as Product ON Product.Product_ID = CAST(Product_ID AS INT64)
            LEFT JOIN
                UNNEST(SPLIT(Document.Disease_ID, ',')) AS Disease_ID
                LEFT JOIN `project-414304.onepage.Disease` as Disease ON Disease.Disease_ID = CAST(Disease_ID AS INT64)
            LEFT JOIN
                UNNEST(SPLIT(Document.Crop_ID, ',')) AS Crop_ID
                LEFT JOIN `project-414304.onepage.Crop` as Crop ON Crop.Crop_ID = CAST(Crop_ID AS INT64)
            LEFT JOIN
                UNNEST(SPLIT(Document.Pest_ID, ',')) AS Pest_ID
                LEFT JOIN `project-414304.onepage.Pest` as Pest ON Pest.Pest_ID = CAST(Pest_ID AS INT64)
            GROUP BY
                Product.Product_Name,
                Document.Title,
                Document.Filetype,
                Document.Modified,
                Document.Year,
                Document.Country,
                Document.Classification,
                Document.Category_for_one_page,
                Document.Application_type,
                Document.Path,
                Document.Filesize,
                Document.Language;"""
        
        query_job = client.query(query)
        results = query_job.result()

        # Convert rows to list of dictionaries
        rows_as_dicts = [dict(row.items()) for row in results]

        print(rows_as_dicts)

        response = {
            'status_code': 'success',
            'data': rows_as_dicts  # Add data to the response
        }   
        return jsonify(response), 200 
        
    except Exception as e:
        response = {
            'status': 'error',
            'message': str(e)
        }
        return jsonify(response), 500
    
if __name__ == '__main__':
    app.run(debug=True)
