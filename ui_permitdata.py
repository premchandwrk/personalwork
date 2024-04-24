@app.route('/search', methods=['POST'])
def search():
    dataset_id = "BCS"
    table_id = "bcs_sp_ui_all_info3"
    data = request.json
    keyword = data.get('keyword')
    schema = get_table_schema(dataset_id, table_id)
    query = f"SELECT * FROM `{dataset_id}.{table_id}` WHERE {' OR '.join([f'CAST({col} AS STRING) LIKE "%{keyword}%"' for col in schema])}"
    results = client.query(query).result()
    return jsonify([dict(row) for row in results])

def get_table_schema(dataset_id, table_id):
    table = client.get_table(f"{dataset_id}.{table_id}")
    return [field.name for field in table.schema]
