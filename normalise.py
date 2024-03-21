import pandas as pd
from google.cloud import bigquery
import logging
from BQconnector.bqconnector import create_or_append_to_bigquery_tables


# def normalize_data(df):
#     """
#     Normalizes the DataFrame into separate tables.

#     Args:
#     - df (DataFrame): Input DataFrame to be normalized.

#     Returns:
#     - tuple: Tuple containing normalized DataFrames for Products, Crops, Diseases, Pests, and Documents.
#     """
#     # Split Product, Crop, Disease, and Pest information
#     product_df = df['Product_names'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Product_Name').drop_duplicates().reset_index(drop=True)
#     product_df['Product_ID'] = product_df.index + 1

#     crop_df = df['crop'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Crop_Name').drop_duplicates().reset_index(drop=True)
#     crop_df['Crop_ID'] = crop_df.index + 1

#     disease_df = df['Diseases'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Disease_Name').drop_duplicates().reset_index(drop=True)
#     disease_df['Disease_ID'] = disease_df.index + 1

#     pest_df = df['pest'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Pest_Name').drop_duplicates().reset_index(drop=True)
#     pest_df['Pest_ID'] = pest_df.index + 1

#     # Create mapping tables
#     productcrop_df = df[['Product_names', 'crop']].assign(Crop_Name=df['crop'].str.split(',')).explode('Crop_Name').merge(product_df, on='Product_names', how='left').merge(crop_df, on='Crop_Name', how='left')[['Product_ID', 'Crop_ID']].drop_duplicates()
#     productdisease_df = df[['Product_names', 'Diseases']].assign(Disease_Name=df['Diseases'].str.split(',')).explode('Disease_Name').merge(product_df, on='Product_names', how='left').merge(disease_df, on='Disease_Name', how='left')[['Product_ID', 'Disease_ID']].drop_duplicates()
#     productpest_df = df[['Product_names', 'pest']].assign(Pest_Name=df['pest'].str.split(',')).explode('Pest_Name').merge(product_df, on='Product_names', how='left').merge(pest_df, on='Pest_Name', how='left')[['Product_ID', 'Pest_ID']].drop_duplicates()
#     documentproduct_df = df[['Product_names']].merge(product_df, on='Product_names', how='left')[['Document_ID', 'Product_ID']].drop_duplicates()


#     # Merge Document information with normalized tables
#     document_df = df.drop(columns=['Product_names', 'crop', 'Diseases', 'pest'])
    
#     document_df['Product_ID'] = df['Product_names'].apply(lambda x: ','.join(str(product_df.loc[product_df['Product_Name'] == p, 'Product_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)
#     document_df['Crop_ID'] = df['crop'].apply(lambda x: ','.join(str(crop_df.loc[crop_df['Crop_Name'] == c, 'Crop_ID'].iloc[0]) for c in x.split(',')) if pd.notnull(x) else None)
#     document_df['Disease_ID'] = df['Diseases'].apply(lambda x: ','.join(str(disease_df.loc[disease_df['Disease_Name'] == d, 'Disease_ID'].iloc[0]) for d in x.split(',')) if pd.notnull(x) else None)
#     document_df['Pest_ID'] = df['pest'].apply(lambda x: ','.join(str(pest_df.loc[pest_df['Pest_Name'] == p, 'Pest_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)

#     return product_df, crop_df, disease_df, pest_df, document_df

import pandas as pd

def normalize_data(df):
    """
    Normalize the DataFrame into separate tables.
    """
    print(df.columns)
    # Split Product, Crop, Disease, and Pest information
    product_df = df['Product_names'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Product_Name').drop_duplicates().reset_index(drop=True)
    product_df['Product_ID'] = product_df.index + 1

    crop_df = df['crop'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Crop_Name').drop_duplicates().reset_index(drop=True)
    crop_df['Crop_ID'] = crop_df.index + 1

    disease_df = df['Diseases'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Disease_Name').drop_duplicates().reset_index(drop=True)
    disease_df['Disease_ID'] = disease_df.index + 1

    pest_df = df['pest'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Pest_Name').drop_duplicates().reset_index(drop=True)
    pest_df['Pest_ID'] = pest_df.index + 1

     # Create Product_Crop mapping table
    product_crop_df = df[['Product_names', 'crop']].copy()
    product_crop_df['Product_names'] = product_crop_df['Product_names'].str.split(',')
    product_crop_df['crop'] = product_crop_df['crop'].str.split(',')
    product_crop_df = product_crop_df.explode('Product_names').explode('crop')
    product_crop_df = product_crop_df.merge(product_df, left_on='Product_names', right_on='Product_Name').merge(crop_df, left_on='crop', right_on='Crop_Name')
    product_crop_df = product_crop_df[['Product_ID', 'Crop_ID']].drop_duplicates().reset_index(drop=True)
    product_crop_df['product_crop_ID'] = product_crop_df.index + 1

    # Create Product_Pest mapping table
    product_pest_df = df[['Product_names', 'pest']].copy()
    product_pest_df['Product_names'] = product_pest_df['Product_names'].str.split(',')
    product_pest_df['pest'] = product_pest_df['pest'].str.split(',')
    product_pest_df = product_pest_df.explode('Product_names').explode('pest')
    product_pest_df = product_pest_df.merge(product_df, left_on='Product_names', right_on='Product_Name').merge(pest_df, left_on='pest', right_on='Pest_Name')
    product_pest_df = product_pest_df[['Product_ID', 'Pest_ID']].drop_duplicates().reset_index(drop=True)
    product_pest_df['product_pest_ID'] = product_pest_df.index + 1

    # Create Product_Disease mapping table
    product_disease_df = df[['Product_names', 'Diseases']].copy()
    product_disease_df['Product_names'] = product_disease_df['Product_names'].str.split(',')
    product_disease_df['Diseases'] = product_disease_df['Diseases'].str.split(',')
    product_disease_df = product_disease_df.explode('Product_names').explode('Diseases')
    product_disease_df = product_disease_df.merge(product_df, left_on='Product_names', right_on='Product_Name').merge(disease_df, left_on='Diseases', right_on='Disease_Name')
    product_disease_df = product_disease_df[['Product_ID', 'Disease_ID']].drop_duplicates().reset_index(drop=True)
    product_disease_df['product_disease_ID'] = product_disease_df.index + 1

    # Merge Document information with normalized tables
    document_df = df.drop(columns=['Product_names', 'crop', 'Diseases', 'pest'])
    document_df['Product_ID'] = df['Product_names'].apply(lambda x: ','.join(str(product_df.loc[product_df['Product_Name'] == p, 'Product_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)
    document_df['Crop_ID'] = df['crop'].apply(lambda x: ','.join(str(crop_df.loc[crop_df['Crop_Name'] == c, 'Crop_ID'].iloc[0]) for c in x.split(',')) if pd.notnull(x) else None)
    document_df['Disease_ID'] = df['Diseases'].apply(lambda x: ','.join(str(disease_df.loc[disease_df['Disease_Name'] == d, 'Disease_ID'].iloc[0]) for d in x.split(',')) if pd.notnull(x) else None)
    document_df['Pest_ID'] = df['pest'].apply(lambda x: ','.join(str(pest_df.loc[pest_df['Pest_Name'] == p, 'Pest_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)

    document_df['document_ID'] = document_df.index + 1

    return product_df, crop_df, disease_df, pest_df, document_df,product_crop_df,product_disease_df,product_pest_df


def extracting_data_as_dataframe(project_id,dataset_id,table_id):
    """
    Extracts data from BigQuery into a DataFrame.

    Args:
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_id (str): BigQuery table ID.

    Returns:
    - DataFrame: DataFrame containing the extracted data.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    df = client.query(query).to_dataframe()
    return df


def insert_data_into_bquery(project_id,dataset_id,table_id):
    """
    Inserts data into BigQuery after normalization.

    Args:
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_id (str): BigQuery table ID.

    Returns:
    - None
    """           
    df = extracting_data_as_dataframe(project_id,dataset_id,table_id)
    logging.info(f"Extracting data from BigQuery {project_id}.{dataset_id}.{table_id} is completed") 

    if not df.empty:
        # Perform data normalization
        df['Modified'] = df['Modified'].astype(str)
        df['Trial_report'] = df['Trial_report'].astype(str)
        product_df, crop_df, disease_df, pest_df, document_df, product_crop_df, product_disease_df, product_pest_df = normalize_data(df)
        logging.info("Normalize data is completed")
        # print(document_df.info())

        # Define tables dictionary
        tables_dict = {
            'Product': product_df,
            'Crop': crop_df,
            'Disease': disease_df,
            'Pest': pest_df,
            'Document': document_df,
            'productcrop':product_crop_df, 'productdisease':product_disease_df, 'productpest':product_pest_df,
        }

        # Create or append to BigQuery tables
        create_or_append_to_bigquery_tables(tables_dict, project_id, dataset_id)
        logging.info("Inserted data is completed")
