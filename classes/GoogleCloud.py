#GoogleCloud

from google.cloud import bigquery
import google.api_core.exceptions as exceptions 
import logging
import pandas as pd
from google.oauth2 import service_account

# LOG LEVEL INFO
logging.basicConfig(level=logging.INFO)

class GoogleCloudClient:
    def __init__(self, service_account_path, write_disposition='WRITE_TRUNCATE', scopes=['https://www.googleapis.com/auth/cloud-platform']):
        self.credentials = service_account.Credentials.from_service_account_file(service_account_path).with_scopes(scopes=scopes)
        self.bq_client = bigquery.Client(credentials=self.credentials)
        self.write_disposition = write_disposition


    def write_to_bigquery_tables(self, data):
        
        #get variables from data object
        df = pd.DataFrame(data.get("data"))    
        table_id = data.get("table_id")
        cols= df.columns
        
        #build load schema
        load_schema = self.load_schema_builder(df)
        
        #create job config
        job_config = self.bq_job_config(load_schema)
        
        # Make an API request.
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  
        
        # Wait for the job to complete.
        job.result()  

        # Make an API request.
        table = self.bq_client.get_table(table_id)  
        
        # Write to log
        load_info = f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        logging.info(load_info)
        
        return load_info


    def bq_job_config(self, load_schema):
        return bigquery.LoadJobConfig(
            write_disposition=self.write_disposition,
            source_format=bigquery.SourceFormat.CSV,
            create_disposition='CREATE_IF_NEEDED', 
            schema=load_schema,
            autodetect=False,
            # not sure about these
            skip_leading_rows=1, # rows to skip in CSV (if you have a header row)
            field_delimiter=",", # CSV delimiter (can also be "\t" for tab delimited or ; for semicolon delimited)
            allow_quoted_newlines=True, # Indicates whether to allow quoted data sections that contain newline characters in a CSV file. The default value is false.
            allow_jagged_rows=True,  # Accept rows that are missing trailing optional columns. The missing values are treated as nulls. If false, records with missing trailing columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. Only applicable to CSV, ignored for other formats.
            max_bad_records=1000, # The maximum number of bad records that BigQuery can ignore when running the job. If the number of bad records exceeds this value, an invalid error is returned in the job result. The default value is 0, which requires that all records are valid.
            ignore_unknown_values=True, # Indicates if BigQuery should allow extra values that are not represented in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. The sourceFormat property determines what BigQuery treats as an extra value:
        )


    def query_bq_table(self, query_string):
        """
        Query BigQuery table and return results as a pandas dataframe
        """
        query_job = self.bq_client.query(query_string)
        results = query_job.result()
        df = results.to_dataframe()
        return df


    def table_exists(self, table_id):
        try:
            self.bq_client.get_table(table_id)
            return True
        except exceptions.NotFound:
            return False


    def load_schema_builder(self, df):
        table_schema = []
        for col in df.columns: # iterate through the cols
            if str(df.dtypes[col]) == "int64":
                table_schema.append(bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.INT64))
            elif str(df.dtypes[col]) == "float64":
                table_schema.append(bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.FLOAT64))
            elif str(df.dtypes[col]) == "bool":
                table_schema.append(bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.BOOL))
            elif str(df.dtypes[col]).startswith("datetime"):
                table_schema.append(bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.DATETIME))
            else:
                table_schema.append(bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.STRING))
        return  table_schema