import pandas as pd
import numpy as np
from utils.aws_connection import AWSConnection
from utils.common import Task
import io
from datetime import datetime
import re

class DataIngestion(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source_name = "mdk"
        self.aws_connection = AWSConnection()

        
    def read_media_plan(self):

        #read_path = f"de_input/{self.source_name}/{self.country_code}/{self.table_name}"
        #bytes_obj, extraction_date, file_name = self.aws_connection.get_most_recent_flat_file_in_bytes(custom_path=read_path)

        filenames = self.aws_connection.get_all_available_file_names_for_one_table_in_s3(self.source_name, self.table_name, input_folder=True)
        partial_dfs = []
        for filename in filenames:

            obj = self.aws_connection.client.get_object(
                    Bucket=self.aws_connection.s3_bucket_name, 
                    Key=f"de_input/{self.source_name}/{self.country_code}/{self.table_name}/{filename}.xlsx"
            )
                

            print(f"Reading {filename}")
            df = pd.read_excel(io.BytesIO(obj['Body'].read()), skipfooter=2)
            #print(f"Reading {file_name} | {extraction_date}")

            if obj is None:
                print(f"[SDG-eComm] Nothing to ingest")

                return
            else:
                def process_pzn(value):
                    # Handling cases with multiple PZNs and unclean values
                    value = str(value).strip()
                    if ',' in value:
                        # Splitting several PZNs to later create as many rows as values
                        return [
                            re.match(r'^(\d+)', v.strip()).group(1)
                            for v in value.split(',')
                            if re.match(r'^(\d+)', v.strip())
                        ]
                    else:
                        # In case noise was typed into the PZN field, extracting just the PZN
                        match = re.match(r'^(\d+)', value)
                        return [match.group(1)] if match else []

                df['PZN'] = df['PZN'].apply(process_pzn)
                df = df.explode('PZN').reset_index(drop=True)

                # The explode above created copies of IDs
                # Thus we get 2025-267-1, 2025-267-2, etc. instead of copies of 2025-267, for example
                duplicated_ids = df['ID'].duplicated(keep=False)

                # Apply suffixes only to duplicated IDs
                df.loc[duplicated_ids, 'ID'] = (
                    df[duplicated_ids]
                    .groupby('ID')
                    .cumcount()
                    .add(1)
                    .astype(str)
                    .radd(df.loc[duplicated_ids, 'ID'] + '-')
                )

                df['PZN'] = df['PZN'].replace('nan', '')
                partial_dfs.append(df.drop_duplicates())
        final_df = pd.concat(partial_dfs)
        return final_df
    

    def read_sellout(self):
        """ Reads sellout dataset from de_input/ folder in S3 """

        #read_path = f"de_input/{self.source_name}/{self.country_code}/{self.table_name}"
        #bytes_obj, extraction_date, file_name = self.aws_connection.get_most_recent_flat_file_in_bytes(custom_path=read_path)
        filenames = self.aws_connection.get_all_available_file_names_for_one_table_in_s3(self.source_name, self.table_name, input_folder=True)
        partial_dfs = []
        for filename in filenames:

            obj = self.aws_connection.client.get_object(
                    Bucket=self.aws_connection.s3_bucket_name, 
                    Key=f"de_input/{self.source_name}/{self.country_code}/{self.table_name}/{filename}.xlsx"
            )
                

            print(f"Reading {filename}")
            df = pd.read_excel(io.BytesIO(obj['Body'].read()), skipfooter=2)



            #df = pd.read_excel(bytes_obj, skipfooter=2)
            df = df.iloc[:, :-1]

            if obj is None:
                print(f"[SDG-eComm] Nothing to ingest")

                return
            else:
                date_headers = df.columns[7:]  

                data = df.iloc[1:].copy()  # Skip the first row
                base_columns = df.iloc[0, 0:7].values  # Columns A to G from second row
                final_columns = list(base_columns) + list(date_headers)
                data.columns = final_columns

                id_vars = final_columns[:7]     # Keep PZN to AEP
                value_vars = final_columns[7:]  # These are date columns (1/1/2023, etc.)

                df_melted = data.melt(
                    id_vars=id_vars,
                    var_name="Date",
                    value_name="Value"
                )

                print(df_melted.head())
                partial_dfs.append(df_melted)
        final_df = pd.concat(partial_dfs)
        final_df = final_df.drop_duplicates(subset=["Name", "PZN", "Hersteller", "Date"])
        return final_df


    def read_inventory(self):
        """ Reads inventory file from de_input/ folder in S3 """
        
        self.aws_connection.root_folder = "de_input"
        filenames = self.aws_connection.get_all_available_file_names_for_one_table_in_s3(self.source_name, self.table_name)
        self.aws_connection.root_folder = "de"

        if len(filenames) == 0:
            print(f"[SDG-eComm] Nothing to ingest")

            return
        else:
            dates = [filename.split('_')[-1] for filename in filenames]
            dates = [datetime.strptime(date, '%d.%m.%y').date() for date in dates]

            latest_date = max(dates)

            files_to_get = [
                filename for filename in filenames 
                if datetime.strptime(filename.split('_')[-1], '%d.%m.%y').date() == latest_date
            ]

            partial_dfs = []
            for filename in files_to_get:
                obj = self.aws_connection.client.get_object(
                                Bucket=self.aws_connection.s3_bucket_name, 
                                Key=f"de_input/{self.source_name}/{self.country_code}/{self.table_name}/{filename}.xlsx"
                )

                partial_df = pd.read_excel(io.BytesIO(obj['Body'].read()), header=0)

                if partial_df.iloc[0, 0] == "PZN": # If an extra index row is provided, skip it
                    partial_df.columns = partial_df.iloc[0]
                    partial_df = partial_df[1:].reset_index(drop=True)

                partial_dfs.append(partial_df)

            df = pd.concat(partial_dfs, ignore_index=True)

            stock_cols = [col for col in df.columns if str(col).endswith("Menge")]
            df["ORDERED_UNITS"] = df[stock_cols].sum(axis=1)

            df = df[["PZN", "Pck.", "Bezeichnung", "ORDERED_UNITS"]]

            grouped_df = df.groupby(["PZN", "Pck.", "Bezeichnung"], as_index=False)["ORDERED_UNITS"].sum()
            grouped_df["DATE"] = latest_date

            print(grouped_df.head())

            return grouped_df


    def write_csv_in_s3(self, df, dataset):
        today = datetime.now().strftime('%Y%m%d')
        aws = self.aws_connection
        
        write_path = f"de/{self.source_name}/{self.country_code}/{dataset}/{dataset}_{today}.csv"

        aws.client.put_object(
            Body=bytes(df.to_csv(index=False), encoding='utf-8'),
            Bucket=aws.s3_bucket_name,
            Key=write_path,
        )


    def full_process(self):
        if self.table_name == "mdk_media_plan":
            df = self.read_media_plan()
        if self.table_name == "mdk_target_units":
            df = self.read_sellout()
        if self.table_name == "mdk_inventory":
            df = self.read_inventory()
        
        if df is not None:
            print(df.head())
            print("="*15)
            print(df.tail())
            print(df.shape[0])
            self.aws_connection.move_previous_csv_to_processed_folder([], self.source_name, self.table_name)
            self.write_csv_in_s3(df, self.table_name)


    def launch(self):
        self.full_process()


def entrypoint(): 
    task = DataIngestion()
    task.launch()


if __name__ == "__main__":
    entrypoint()
