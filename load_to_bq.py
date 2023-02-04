import pandas as pd
from classes.GoogleCloud import GoogleCloudClient
import os
from dotenv import load_dotenv

load_dotenv()

# Load the example file
df = pd.read_csv('export_data/task_details.csv')

# load sevice account
service_account = 'service_accounts/sa.json'

# Create a dictionary to pass to the write_to_bigquery_tables method
data = {
    "table_id": f"{os.getenv('BIGQUERY_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID')}.{os.getenv('BIGQUERY_TABLE_ID')}",
    "data": df
}

# Initialize the GoogleCloudClient
gcc = GoogleCloudClient(service_account_path=service_account)

# Write the data to BigQuery
gcc.write_to_bigquery_tables(data=data, write_disposition='WRITE_TRUNCATE')