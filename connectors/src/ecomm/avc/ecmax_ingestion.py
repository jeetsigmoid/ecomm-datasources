
import pandas as pd
import numpy as np
from utils.aws_connection import AWSConnection
from utils.common import Task
import io
from datetime import datetime


class DataIngestion(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source_name = "ecmax"
        self.aws_connection = AWSConnection()


    def read_input(self, dataset):
        if self.table_name != 'ecmax_inventory':
            bytes_obj, extraction_date, file_name = self.aws_connection.get_most_recent_flat_file_in_bytes(custom_path=f"de_input/{self.source_name}/{self.country_code}/{self.table_name}")
            format = self.pipeline_config['format']
            if format == '.xlsx':
                df = pd.read_excel(bytes_obj)
            else:
                df = pd.read_csv(bytes_obj)
        else:
            print('Nothing happens')
            return None

        return df


    def read_input_(self, dataset):

        # if dataset == 'ryc_target_units':
        #     table = "ryc_sellout"
        
        table = dataset
        print(f"de_input/{self.source_name}/{self.country_code}/{table}/")
        filenames = self.aws_connection.get_all_available_file_names_for_one_table_in_s3(self.source_name, table)
        if len(filenames):
            partial_dfs = []
            for filename in filenames:                    
                format = self.pipeline_config['format']

                obj = self.aws_connection.client.get_object(
                    Bucket=self.aws_connection.s3_bucket_name, 
                    Key=f"de_input/{self.source_name}/{self.country_code}/{table}/{filename}{format}")
                

                print(f"de_input/{self.source_name}/{self.country_code}/{table}/{filename}{format}")
                if format == '.xlsx':
                    df = pd.read_excel(io.BytesIO(obj['Body'].read()))
                else:
                    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

                partial_dfs.append(df)

            return pd.concat(partial_dfs, ignore_index=True)
        
        else: 
            print(f"Nothing to ingest")
    

    def clean_data(self, df):
        date_cols = [col for col in df.columns if col.endswith('date')]
        numeric_cols = [col for col in df.columns if col.endswith('units') or col.endswith('value_eur')]

        for col in date_cols:
            df[col] =  pd.to_datetime(df[col], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        for col in numeric_cols:
            df[col] = df[col].map(lambda x: np.nan if x == '[NULL]' else x)

        df['COUNTRY_CODE'] = self.country_code
        df['last_updated_timestamp'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        if 'Year Month' in df.columns:
            df['Year Month'] = df['Year Month'].fillna(0).astype(int)
        
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
            df = df.dropna(subset=['Year'])

        # SKU not NULL or empty
        if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip()  # Convertir a string y eliminar espacios
                df = df[df['SKU'].notna() & (df['SKU'] != '') & (df['SKU'].str.lower() != 'null')]

        return df


    def write_csv_in_s3(self, df, dataset):
        today = datetime.now().strftime('%Y%m%d')
        aws = self.aws_connection
        
        write_path = f"de/{self.source_name}/{self.country_code}/{dataset}/{dataset}_{today}.csv"

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=True)

        aws.client.put_object(
            Body=csv_buffer.getvalue(),
            # Body=bytes(df.to_csv(index=False), encoding='utf-8'),
            Bucket=aws.s3_bucket_name,
            Key=write_path,
        )


    def full_process(self):
        debug_prefix = f"[SDG-eComm] "
        print(f"{debug_prefix}CN ECMAX data ingestion started")
        dataset = self.table_name
        print(f"{debug_prefix}\tReading {dataset}")
        df = self.read_input(dataset)
        if df is not None:
            print(f"{debug_prefix}\t\tFound file with shape {df.shape}")
            df = self.clean_data(df)
            self.aws_connection.move_previous_csv_to_processed_folder([], self.source_name, self.table_name)
            print(f"{debug_prefix}\t\tWriting clean CSV in S3")
            self.write_csv_in_s3(df, dataset)
        print(f"{debug_prefix}\t\tFinished {dataset}")
        print(f"{debug_prefix}CN ECMAX ingested")


    def launch(self):
        self.full_process()

def entrypoint(): 
    task = DataIngestion()
    task.launch()


if __name__ == "__main__":
    entrypoint()
