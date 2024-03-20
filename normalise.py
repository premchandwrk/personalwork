def normalize_data(df):
    """
    Normalize the DataFrame into separate tables and generate mapping tables.
    """
    # Split Product, Crop, Disease, and Pest information
    product_df = df['Product_names'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Product_Name').drop_duplicates().reset_index(drop=True)
    product_df['Product_ID'] = product_df.index + 1

    crop_df = df['crop'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Crop_Name').drop_duplicates().reset_index(drop=True)
    crop_df['Crop_ID'] = crop_df.index + 1

    disease_df = df['Diseases'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Disease_Name').drop_duplicates().reset_index(drop=True)
    disease_df['Disease_ID'] = disease_df.index + 1

    pest_df = df['pest'].str.split(',').explode().str.strip().reset_index(drop=True).to_frame('Pest_Name').drop_duplicates().reset_index(drop=True)
    pest_df['Pest_ID'] = pest_df.index + 1

    # Create mapping tables
    product_crop_df = df[['Product_names', 'crop']].explode('Product_names').explode('crop').reset_index(drop=True)
    product_crop_df = product_crop_df.merge(product_df, left_on='Product_names', right_on='Product_Name', how='left')
    product_crop_df = product_crop_df.merge(crop_df, left_on='crop', right_on='Crop_Name', how='left')
    product_crop_df = product_crop_df[['Product_ID', 'Crop_ID']].drop_duplicates().reset_index(drop=True)
    product_crop_df['ProductCrop_ID'] = product_crop_df.index + 1

    product_disease_df = df[['Product_names', 'Diseases']].explode('Product_names').explode('Diseases').reset_index(drop=True)
    product_disease_df = product_disease_df.merge(product_df, left_on='Product_names', right_on='Product_Name', how='left')
    product_disease_df = product_disease_df.merge(disease_df, left_on='Diseases', right_on='Disease_Name', how='left')
    product_disease_df = product_disease_df[['Product_ID', 'Disease_ID']].drop_duplicates().reset_index(drop=True)
    product_disease_df['ProductDisease_ID'] = product_disease_df.index + 1

    product_pest_df = df[['Product_names', 'pest']].explode('Product_names').explode('pest').reset_index(drop=True)
    product_pest_df = product_pest_df.merge(product_df, left_on='Product_names', right_on='Product_Name', how='left')
    product_pest_df = product_pest_df.merge(pest_df, left_on='pest', right_on='Pest_Name', how='left')
    product_pest_df = product_pest_df[['Product_ID', 'Pest_ID']].drop_duplicates().reset_index(drop=True)
    product_pest_df['ProductPest_ID'] = product_pest_df.index + 1

    document_product_df = df[['path', 'Product_names']].explode('Product_names').reset_index(drop=True)
    document_product_df = document_product_df.merge(product_df, left_on='Product_names', right_on='Product_Name', how='left')
    document_product_df = document_product_df[['path', 'Product_ID']].drop_duplicates().reset_index(drop=True)
    document_product_df['DocumentProduct_ID'] = document_product_df.index + 1

    # Merge Document information with normalized tables
    document_df = df.drop(columns=['Product_names', 'crop', 'Diseases', 'pest'])
    document_df['Product_ID'] = df['Product_names'].str.split(',').apply(lambda x: ','.join(str(product_df.loc[product_df['Product_Name'] == p.strip(), 'Product_ID'].iloc[0]) for p in x) if pd.notnull(x) else None)
    document_df['Crop_ID'] = df['crop'].str.split(',').apply(lambda x: ','.join(str(crop_df.loc[crop_df['Crop_Name'] == c.strip(), 'Crop_ID'].iloc[0]) for c in x) if pd.notnull(x) else None)
    document_df['Disease_ID'] = df['Diseases'].str.split(',').apply(lambda x: ','.join(str(disease_df.loc[disease_df['Disease_Name'] == d.strip(), 'Disease_ID'].iloc[0]) for d in x) if pd.notnull(x) else None)
    document_df['Pest_ID'] = df['pest'].str.split(',').apply(lambda x: ','.join(str(pest_df.loc
