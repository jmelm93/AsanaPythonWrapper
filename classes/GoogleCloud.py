#GoogleCloud

from google.cloud import bigquery
import google.api_core.exceptions as exceptions 
import logging
import pandas as pd
from google.oauth2 import service_account

# LOG LEVEL INFO
logging.basicConfig(level=logging.INFO)

class GoogleCloudClient:
    def __init__(
        self, 
        service_account_path, 
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    ):
        self.credentials = service_account.Credentials.from_service_account_file(service_account_path).with_scopes(scopes=scopes)
        self.bq_client = bigquery.Client(credentials=self.credentials)


    def table_exists(self, table_id):
        try:
            self.bq_client.get_table(table_id)
            return True
        except exceptions.NotFound:
            return False


    def write_to_bigquery_tables(self, data, write_disposition="WRITE_APPEND", partition_field=None):
        table_id = data.get("table_id") 
        # check if table exists
        if not self.table_exists(table_id):
            # if not, create it
            self.create_table_with_table_id_and_schema(table_id, data.get("data").columns)
        df = pd.DataFrame(data.get("data"))
        job_config = self.bq_job_config(write_disposition, partition_field)
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.
        table = self.bq_client.get_table(table_id)  # Make an API request.
        load_info = f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        logging.info(load_info)
        
        return load_info


    def bq_job_config(self, write_disposition, partition_field):
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1, # rows to skip in CSV (if you have a header row)
            autodetect=True, # auto detect schema types (sometimes it doesn't work - may want to adjust to hard code schema)
            field_delimiter=",", # CSV delimiter (can also be "\t" for tab delimited or ; for semicolon delimited)
            allow_quoted_newlines=True, # Indicates whether to allow quoted data sections that contain newline characters in a CSV file. The default value is false.
            allow_jagged_rows=True,  # Accept rows that are missing trailing optional columns. The missing values are treated as nulls. If false, records with missing trailing columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. Only applicable to CSV, ignored for other formats.
            max_bad_records=1000, # The maximum number of bad records that BigQuery can ignore when running the job. If the number of bad records exceeds this value, an invalid error is returned in the job result. The default value is 0, which requires that all records are valid.
            ignore_unknown_values=True, # Indicates if BigQuery should allow extra values that are not represented in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. The sourceFormat property determines what BigQuery treats as an extra value:
        )
        # only create partition if one if provided
        if partition_field is not None:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        return job_config


    def create_table_with_table_id_and_schema(self, table_id, table_schema):
        """
        Check if table exists in BigQuery. If not, create it.
        """
        if not self.table_exists(table_id):
            schema = self.convert_list_of_column_names_to_schema_object(table_schema)
            table = bigquery.Table(table_id, schema=schema)
            table = self.bq_client.create_table(table)
            logging.info(f"Table {table_id} created")
            return True
        else:
            logging.info(f"Table {table_id} already exists")


    def convert_list_of_column_names_to_schema_object(self, column_names):
        schema = []
        for column_name in column_names:
            schema.append({"name": column_name, "type": "STRING"})
        return schema
