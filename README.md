# personalwork

# Excel to BigQuery Loader

## Introduction
This script loads data from an Excel file into a BigQuery table. It truncates the existing table and replaces it with the new data from the Excel file. The script enforces a master schema for the BigQuery table and only inserts columns that match the schema.

## Requirements
- Python 3.x
- pandas
- pandas-gbq
- Google Cloud Platform account with BigQuery access

## Installation
1. Clone the repository or download the script file.
2. Install the required Python libraries:

    ```sh
    pip install -r reqirements.txt
    ```
4. Set up Google Cloud Platform credentials for authentication with BigQuery.

## Usage
1. Replace the placeholders in the script with your specific values:
- `excel_file`: Path to your Excel file.
- `sheet_name`: Name or index of the sheet in the Excel file.
- `project_id`: Your Google Cloud project ID.
- `dataset_id`: ID of the BigQuery dataset where the table resides.
- `table_id`: ID of the BigQuery table.
- `MASTER_SCHEMA`: Define the master schema for the BigQuery table.
2. Run the script:
  
      ```sh
    python main.py
      ```

## Functionality
- The script reads data from the specified Excel file into a Pandas DataFrame.
- It truncates the existing BigQuery table to remove all existing data.
- It creates a new BigQuery table with the same schema as the master schema defined in the script.
- Only columns from the Excel file that match the master schema are inserted into the BigQuery table.
- Any new columns appearing in the Excel file are ignored during insertion.
- The script prints error messages if the Excel file is not found, if the file format is invalid, or if there is an error during table creation or data insertion.

## Notes
- Ensure that your Google Cloud Platform account has the necessary permissions to create and write to BigQuery datasets and tables.
- Make sure the Excel file is in a supported format and contains data in the specified sheet.
- Verify that the master schema defined in the script matches the structure of the data in the Excel file.

